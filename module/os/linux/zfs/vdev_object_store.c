/*
 * CDDL HEADER START
 *
 * The contents of this file are subject to the terms of the
 * Common Development and Distribution License (the "License").
 * You may not use this file except in compliance with the License.
 *
 * You can obtain a copy of the license at usr/src/OPENSOLARIS.LICENSE
 * or http://www.opensolaris.org/os/licensing.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * When distributing Covered Code, include this CDDL HEADER in each
 * file and include the License file at usr/src/OPENSOLARIS.LICENSE.
 * If applicable, add the following below this CDDL HEADER, with the
 * fields enclosed by brackets "[]" replaced with your own identifying
 * information: Portions Copyright [yyyy] [name of copyright owner]
 *
 * CDDL HEADER END
 */
/*
 * Copyright (c) 2021 by Delphix. All rights reserved.
 */

#include <sys/zfs_context.h>
#include <sys/spa.h>
#include <sys/spa_impl.h>
#include <sys/vdev_impl.h>
#include <sys/vdev_trim.h>
#include <sys/zio.h>
#include <sys/fs/zfs.h>
#include <sys/fm/fs/zfs.h>
#include <sys/abd.h>
#include <sys/fcntl.h>
#ifdef _KERNEL
#include <linux/un.h>
#include <linux/net.h>
#endif
/*
 * Virtual device vector for object storage.
 */

/*
 * Possible keys in nvlist requests / responses to/from the Agent
 */
#define	AGENT_TYPE		"Type"
#define	AGENT_TYPE_CREATE_POOL		"create pool"
#define	AGENT_TYPE_OPEN_POOL		"open pool"
#define	AGENT_TYPE_OPEN_POOL		"open pool"
#define	AGENT_TYPE_READ_BLOCK		"read block"
#define	AGENT_TYPE_WRITE_BLOCK		"write block"
#define	AGENT_TYPE_FREE_BLOCK		"free block"
#define	AGENT_TYPE_BEGIN_TXG		"begin txg"
#define	AGENT_TYPE_END_TXG		"end txg"
#define	AGENT_TYPE_FLUSH_WRITES		"flush writes"
#define	AGENT_NAME		"name"
#define	AGENT_SIZE		"size"
#define	AGENT_TXG		"TXG"
#define	AGENT_GUID		"GUID"
#define	AGENT_BUCKET		"bucket"
#define	AGENT_BLKID		"block"
#define	AGENT_DATA		"data"
#define	AGENT_REQUEST_ID	"request_id"

/*
 * By default, the logical/physical ashift for object store vdevs is set to
 * SPA_MINBLOCKSHIFT (9). This allows all object store vdevs to use
 * 512B (1 << 9) blocksizes. Users may opt to change one or both of these
 * for testing or performance reasons. Care should be taken as these
 * values will impact the vdev_ashift setting which can only be set at
 * vdev creation time.
 */
unsigned long vdev_object_store_logical_ashift = SPA_MINBLOCKSHIFT;
unsigned long vdev_object_store_physical_ashift = SPA_MINBLOCKSHIFT;

#define	VOS_MAXREQ	100

typedef struct vdev_object_store {
	char *vos_access_id;
	char *vos_secrete_key;
	struct socket *vos_sock;
	zio_t *vos_outstanding_requests[VOS_MAXREQ];
	kthread_t *vos_reader;
} vdev_object_store_t;

static mode_t
vdev_object_store_open_mode(spa_mode_t spa_mode)
{
	mode_t mode = 0;

	if ((spa_mode & SPA_MODE_READ) && (spa_mode & SPA_MODE_WRITE)) {
		mode = O_RDWR;
	} else if (spa_mode & SPA_MODE_READ) {
		mode = O_RDONLY;
	} else if (spa_mode & SPA_MODE_WRITE) {
		mode = O_WRONLY;
	}

	return (mode);
}

#ifdef _KERNEL
static struct sockaddr_un zfs_socket = {
	AF_UNIX, "/run/zfs_socket"
};
#endif

static int
zfs_object_store_open(char *bucket_name, int mode, struct socket **sock)
{
	struct socket *s = NULL;
	int rc = sock_create(PF_UNIX, SOCK_STREAM, 0, &s);
	if (rc != 0) {
		zfs_dbgmsg("zfs_object_store_open unable to create "
		    "socket: %d", rc);
		return (rc);
	}

	rc = s->ops->connect(s, (struct sockaddr *)&zfs_socket,
	    sizeof (zfs_socket), 0);
	if (rc != 0) {
		zfs_dbgmsg("zfs_object_store_open failed to connect: %d", rc);
		sock_release(s);
		s = NULL;
		return (rc);
	}

	*sock = s;
	zfs_dbgmsg("zfs_object_store_open, socket connection ready");
	return (0);
}

static void
zfs_object_store_close(struct socket *sock)
{
	sock_release(sock);
	sock = NULL;
}

static void
agent_request(vdev_object_store_t *vos, nvlist_t *nv)
{
	struct msghdr msg = {};
	struct kvec iov[2] = {};
	size_t iov_size = 0;
	char *iov_buf = fnvlist_pack(nv, &iov_size);
	uint64_t size64 = iov_size;
	zfs_dbgmsg("sending %llu-byte request to agent type=%s",
	    size64, fnvlist_lookup_string(nv, AGENT_TYPE));

	iov[0].iov_base = &size64;
	iov[0].iov_len = sizeof (size64);
	iov[1].iov_base = iov_buf;
	iov[1].iov_len = iov_size;
	uint64_t total_size = sizeof (size64) + iov_size;
	// XXX need locking on socket?
	VERIFY3U(kernel_sendmsg(vos->vos_sock, &msg, iov,
	    2, total_size), ==, total_size);

	fnvlist_pack_free(iov_buf, iov_size);
}

/*
 * Send request to agent; returns request ID (index in
 * vos_outstanding_requests).  nvlist may be modified.
 */
static uint64_t
agent_request_zio(vdev_object_store_t *vos, zio_t *zio, nvlist_t *nv)
{
	uint64_t req;

	/*
	 * XXX need locking on requests array since this could be
	 * called concurrently
	 */
	for (req = 0; req < VOS_MAXREQ; req++) {
		if (vos->vos_outstanding_requests[req] == NULL) {
			vos->vos_outstanding_requests[req] = zio;
			break;
		}
	}
	/*
	 * XXX if all slots are full, need to block and wait for
	 * one to complete
	 */
	VERIFY3U(req, <, VOS_MAXREQ);
	fnvlist_add_uint64(nv, AGENT_REQUEST_ID, req);
	zio->io_vsd = (void *)(uintptr_t)req;
	zfs_dbgmsg("agent_request_zio(req=%llu)",
	    req);
	agent_request(vos, nv);
	return (req);
}

/*
 * Send read request to agent; returns request ID (index in
 * vos_outstanding_requests).
 */
static void
agent_read_block(vdev_object_store_t *vos, zio_t *zio)
{
	uint64_t blockid = zio->io_offset >> 9;
	nvlist_t *nv = fnvlist_alloc();
	fnvlist_add_string(nv, AGENT_TYPE, AGENT_TYPE_READ_BLOCK);
	fnvlist_add_uint64(nv, AGENT_BLKID, blockid);
	zfs_dbgmsg("agent_read_block(guid=%llu blkid=%llu)",
	    spa_guid(zio->io_spa), blockid);
	agent_request_zio(vos, zio, nv);
	fnvlist_free(nv);
}

static void
agent_write_block(vdev_object_store_t *vos, zio_t *zio)
{
	uint64_t blockid = zio->io_offset >> 9;
	nvlist_t *nv = fnvlist_alloc();
	fnvlist_add_string(nv, AGENT_TYPE, AGENT_TYPE_WRITE_BLOCK);
	fnvlist_add_uint64(nv, AGENT_BLKID, blockid);
	void *buf = abd_borrow_buf_copy(zio->io_abd, zio->io_size);
	fnvlist_add_uint8_array(nv, AGENT_DATA, buf, zio->io_size);
	abd_return_buf(zio->io_abd, buf, zio->io_size);
	zfs_dbgmsg("agent_write_block(guid=%llu blkid=%llu len=%llu)",
	    spa_guid(zio->io_spa),
	    blockid,
	    zio->io_size);
	agent_request_zio(vos, zio, nv);
	fnvlist_free(nv);
}

static void
agent_create_pool(vdev_t *vd, vdev_object_store_t *vos)
{
	nvlist_t *nv = fnvlist_alloc();
	fnvlist_add_string(nv, AGENT_TYPE, AGENT_TYPE_CREATE_POOL);
	fnvlist_add_string(nv, AGENT_NAME, spa_name(vd->vdev_spa));
	fnvlist_add_uint64(nv, AGENT_GUID, spa_guid(vd->vdev_spa));
	fnvlist_add_string(nv, AGENT_BUCKET, vd->vdev_path);
	zfs_dbgmsg("agent_create_pool(guid=%llu name=%s bucket=%s)",
	    spa_guid(vd->vdev_spa),
	    spa_name(vd->vdev_spa),
	    vd->vdev_path);
	agent_request(vos, nv);
	fnvlist_free(nv);
}

static void
agent_open_pool(vdev_t *vd, vdev_object_store_t *vos)
{
	nvlist_t *nv = fnvlist_alloc();
	fnvlist_add_string(nv, AGENT_TYPE, AGENT_TYPE_OPEN_POOL);
	fnvlist_add_uint64(nv, AGENT_GUID, spa_guid(vd->vdev_spa));
	fnvlist_add_string(nv, AGENT_BUCKET, vd->vdev_path);
	zfs_dbgmsg("agent_open_pool(guid=%llu bucket=%s)",
	    spa_guid(vd->vdev_spa),
	    vd->vdev_path);
	agent_request(vos, nv);
	fnvlist_free(nv);
}

static void
agent_begin_txg(vdev_object_store_t *vos, uint64_t txg)
{
	nvlist_t *nv = fnvlist_alloc();
	fnvlist_add_string(nv, AGENT_TYPE, AGENT_TYPE_BEGIN_TXG);
	fnvlist_add_uint64(nv, AGENT_TXG, txg);
	zfs_dbgmsg("agent_begin_txg(%llu)",
	    txg);
	agent_request(vos, nv);
	fnvlist_free(nv);
}

static void
agent_end_txg(vdev_object_store_t *vos, uint64_t txg, void *buf, size_t len)
{
	nvlist_t *nv = fnvlist_alloc();
	fnvlist_add_string(nv, AGENT_TYPE, AGENT_TYPE_END_TXG);
	fnvlist_add_uint64(nv, AGENT_TXG, txg);
	fnvlist_add_uint8_array(nv, AGENT_DATA, buf, len);
	zfs_dbgmsg("agent_end_txg(%llu)",
	    txg);
	agent_request(vos, nv);
	fnvlist_free(nv);
}

void
object_store_begin_txg(spa_t *spa, uint64_t txg)
{
	vdev_t *vd = spa->spa_root_vdev->vdev_child[0];
	ASSERT3P(vd->vdev_ops, ==, &vdev_object_store_ops);
	vdev_object_store_t *vos = vd->vdev_tsd;
	agent_begin_txg(vos, txg);
}

void
object_store_end_txg(spa_t *spa, uint64_t txg)
{
	vdev_t *vd = spa->spa_root_vdev->vdev_child[0];
	ASSERT3P(vd->vdev_ops, ==, &vdev_object_store_ops);
	vdev_object_store_t *vos = vd->vdev_tsd;
	//size_t nvlen;
	//char *nvbuf = fnvlist_pack(spa->spa_config_syncing, &nvlen);
	agent_end_txg(vos, txg,
	    &spa->spa_uberblock, sizeof (spa->spa_uberblock));
	// XXX wait for response
	delay(hz * 5);

	//fnvlist_pack_free(nvbuf, nvlen);
}

static void
agent_read_all(vdev_object_store_t *vos, void *buf, size_t len)
{
	struct msghdr msg = {};
	struct kvec iov = {};

	iov.iov_base = buf;
	iov.iov_len = len;

	// XXX need a loop to keep trying to read?
	VERIFY3S(kernel_recvmsg(vos->vos_sock, &msg, &iov, 1, len, 0), ==, len);
}

static void
agent_reader(void *arg)
{
	vdev_object_store_t *vos = arg;

	for (;;) {
		uint64_t nvlist_len;
		agent_read_all(vos, &nvlist_len, sizeof (nvlist_len));
		void *buf = kmem_alloc(nvlist_len, KM_SLEEP);
		agent_read_all(vos, buf, nvlist_len);
		nvlist_t *nv = fnvlist_unpack(buf, nvlist_len);
		kmem_free(buf, nvlist_len);

		const char *type = fnvlist_lookup_string(nv, AGENT_TYPE);
		zfs_dbgmsg("got response from agent type=%s", type);
		// XXX debug message the nvlist
		if (strcmp(type, "pool create done") == 0) {
			zfs_dbgmsg("got pool create done");
		} else if (strcmp(type, "pool open done") == 0) {
			zfs_dbgmsg("got pool open done");
		} else if (strcmp(type, "read done") == 0) {
			uint64_t req = fnvlist_lookup_uint64(nv,
			    AGENT_REQUEST_ID);
			uint_t len;
			void *data = fnvlist_lookup_uint8_array(nv,
			    AGENT_DATA, &len);
			zfs_dbgmsg("got read done req=%llu datalen=%u",
			    req, len);
			VERIFY3U(req, <, VOS_MAXREQ);
			zio_t *zio = vos->vos_outstanding_requests[req];
			VERIFY3U((uintptr_t)zio->io_vsd, ==, req);
			VERIFY3U(fnvlist_lookup_uint64(nv, AGENT_BLKID), ==,
			    zio->io_offset >> 9);
			zio->io_vsd = NULL;
			vos->vos_outstanding_requests[req] = NULL;
			VERIFY3U(len, ==, zio->io_size);
			VERIFY3U(len, ==, abd_get_size(zio->io_abd));
			abd_copy_from_buf(zio->io_abd, data, len);
			fnvlist_free(nv);
			zio_delay_interrupt(zio);
		} else if (strcmp(type, "write done") == 0) {
			uint64_t req = fnvlist_lookup_uint64(nv,
			    AGENT_REQUEST_ID);
			zfs_dbgmsg("got write done req=%llu", req);
			VERIFY3U(req, <, VOS_MAXREQ);
			zio_t *zio = vos->vos_outstanding_requests[req];
			VERIFY3U((uintptr_t)zio->io_vsd, ==, req);
			VERIFY3U(fnvlist_lookup_uint64(nv, AGENT_BLKID), ==,
			    zio->io_offset >> 9);
			zio->io_vsd = NULL;
			vos->vos_outstanding_requests[req] = NULL;
			fnvlist_free(nv);
			zio_delay_interrupt(zio);
		} else {
			zfs_dbgmsg("unrecognized response type!");
		}
	}
}

static int
vdev_object_store_open(vdev_t *vd, uint64_t *psize, uint64_t *max_psize,
    uint64_t *logical_ashift, uint64_t *physical_ashift)
{
	vdev_object_store_t *vos;
	struct socket *sock;
	int error;

	/*
	 * Rotational optimizations only make sense on block devices.
	 */
	vd->vdev_nonrot = B_TRUE;

	/*
	 * Allow TRIM on object store based vdevs.  This may not always
	 * be supported, since it depends on your kernel version and
	 * underlying filesystem type but it is always safe to attempt.
	 */
	vd->vdev_has_trim = B_FALSE;

	/*
	 * Disable secure TRIM on object store based vdevs.
	 */
	vd->vdev_has_securetrim = B_FALSE;

	/*
	 * We use the pathname to specfiy the object store name.
	 */
	if (vd->vdev_path == NULL) {
		vd->vdev_stat.vs_aux = VDEV_AUX_BAD_LABEL;
		return (SET_ERROR(EINVAL));
	}

	/*
	 * Reopen the device if it's not currently open.  Otherwise,
	 * just update the physical size of the device.
	 */
	if (vd->vdev_tsd != NULL) {
		ASSERT(vd->vdev_reopening);
		vos = vd->vdev_tsd;
		goto skip_open;
	}
	ASSERT(vd->vdev_path != NULL);

	vos = vd->vdev_tsd = kmem_zalloc(sizeof (vdev_object_store_t),
	    KM_SLEEP);

	error = zfs_object_store_open(vd->vdev_path,
	    vdev_object_store_open_mode(spa_mode(vd->vdev_spa)), &sock);
	if (error) {
		vd->vdev_stat.vs_aux = VDEV_AUX_OPEN_FAILED;
		return (error);
	}

	vos->vos_sock = sock;

	vos->vos_reader = thread_create(NULL, 0, agent_reader,
	    vos, 0, &p0, TS_RUN, defclsyspri);

	if (vd->vdev_spa->spa_load_state == SPA_LOAD_CREATE) {
		agent_create_pool(vd, vos);
		// XXX wait for response from agent
		delay(hz * 5);
	}
	agent_open_pool(vd, vos);
	// XXX wait for response from agent
	delay(hz * 5);

skip_open:

	/*
	 * XXX - We can only support ~1EB since the metaslab weights
	 * use some of the high order bits.
	 */
	*max_psize = *psize = (1ULL << 60) - 1;
	*logical_ashift = vdev_object_store_logical_ashift;
	*physical_ashift = vdev_object_store_physical_ashift;

	return (0);
}

static void
vdev_object_store_close(vdev_t *vd)
{
	vdev_object_store_t *vos = vd->vdev_tsd;

	if (vd->vdev_reopening || vos == NULL)
		return;

	if (vos->vos_sock != NULL) {
		zfs_object_store_close(vos->vos_sock);
	}

	vd->vdev_delayed_close = B_FALSE;
	kmem_free(vos, sizeof (vdev_object_store_t));
	vd->vdev_tsd = NULL;
}

static void
vdev_object_store_io_strategy(void *arg)
{
	zio_t *zio = arg;
	vdev_t *vd = zio->io_vd;
	vdev_object_store_t *vos = vd->vdev_tsd;

	if (zio->io_type == ZIO_TYPE_READ) {
		agent_read_block(vos, zio);
	} else {
		ASSERT3U(zio->io_type, ==, ZIO_TYPE_WRITE);
		agent_write_block(vos, zio);
	}
}

static void
vdev_object_store_io_start(zio_t *zio)
{
	vdev_t *vd = zio->io_vd;

	if (zio->io_type == ZIO_TYPE_IOCTL) {
		/* XXPOLICY */
		if (!vdev_readable(vd)) {
			zio->io_error = SET_ERROR(ENXIO);
			zio_interrupt(zio);
			return;
		}

		switch (zio->io_cmd) {
		case DKIOCFLUSHWRITECACHE:

			if (zfs_nocacheflush)
				break;

			/*
			 * XXX - may need a new ioctl sinc this will
			 * sync the entire object store.
			 */
			break;
		default:
			zio->io_error = SET_ERROR(ENOTSUP);
		}

		zio_execute(zio);
		return;
	} else if (zio->io_type == ZIO_TYPE_TRIM) {
		/* XXX - Don't support it right now */
		zio->io_error = SET_ERROR(ENOTSUP);
		zio_execute(zio);
		return;
	}

	zio->io_target_timestamp = zio_handle_io_delay(zio);
	vdev_object_store_io_strategy(zio);
}

/* ARGSUSED */
static void
vdev_object_store_io_done(zio_t *zio)
{
}

vdev_ops_t vdev_object_store_ops = {
	.vdev_op_init = NULL,
	.vdev_op_fini = NULL,
	.vdev_op_open = vdev_object_store_open,
	.vdev_op_close = vdev_object_store_close,
	.vdev_op_asize = vdev_default_asize,
	.vdev_op_min_asize = vdev_default_min_asize,
	.vdev_op_min_alloc = NULL,
	.vdev_op_io_start = vdev_object_store_io_start,
	.vdev_op_io_done = vdev_object_store_io_done,
	.vdev_op_state_change = NULL,
	.vdev_op_need_resilver = NULL,
	.vdev_op_hold = NULL,
	.vdev_op_rele = NULL,
	.vdev_op_remap = NULL,
	.vdev_op_xlate = vdev_default_xlate,
	.vdev_op_rebuild_asize = NULL,
	.vdev_op_metaslab_init = NULL,
	.vdev_op_config_generate = NULL,
	.vdev_op_nparity = NULL,
	.vdev_op_ndisks = NULL,
	.vdev_op_type = VDEV_TYPE_OBJSTORE,	/* name of this vdev type */
	.vdev_op_leaf = B_TRUE			/* leaf vdev */
};

ZFS_MODULE_PARAM(zfs_vdev_object_store, vdev_object_store_,
    logical_ashift, ULONG, ZMOD_RW,
	"Logical ashift for object store based devices");
ZFS_MODULE_PARAM(zfs_vdev_object_store, vdev_object_store_,
    physical_ashift, ULONG, ZMOD_RW,
	"Physical ashift for object store based devices");
