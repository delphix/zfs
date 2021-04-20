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

#define	VDEV_OBJECT_STORE_REQUEST	"vdev_object_store_request"
#define	VDEV_OBJECT_STORE_SIZE		"vdev_object_store_size"
#define	VDEV_OBJECT_STORE_BLKID		"vdev_object_store_blkid"
#define	VDEV_OBJECT_STORE_DATA		"vdev_object_store_date"

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

typedef struct vdev_object_store {
	char *vos_access_id;
	char *vos_secrete_key;
	struct socket *vos_sock;
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
		zfs_dbgmsg("zfs_object_store_open unable to create socket: %d", rc);
		return (rc);
	}

	rc = s->ops->connect(s, (struct sockaddr *)&zfs_socket,
	    sizeof (zfs_socket), 0);
	if (rc != 0) {
		zfs_dbgmsg("zfs_object_store_opne failed to connect: %d", rc);
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

#if 0
static int
zfs_object_store_reply() {
	struct msghdr msg = {};
	struct kvec iov = {};

	iov.iov_base = buf;
	iov.iov_len = size;
	iov_iter_kvec(&msg.msg_iter, READ, &iov, 1, size);

	int rc = sock_recvmsg(sock, &msg, 0);
	if (rc != 0) {
		zfs_dbgmsg("zfs_object_store_pread failed: %d", rc);
		return (-rc);
	}

	uint_t len = 0;
	nvlist_t *nv = fnvlist_unpack(buf, size);
	count = fnvlist_lookup_uint64(nv, VDEV_OBJECT_STORE_SIZE);
	off = fnvlist_lookup_uint64(nv, VDEV_OBJECT_STORE_BLKID);
	buf = fnvlist_lookup_uint8_array(nv, VDEV_OBJECT_STORE_DATA, &len);

	fnvlist_free(nv);
	return (0);
}
#endif

static int
zfs_object_store_request(struct socket *sock, zio_type_t type, void *buf,
    size_t size, loff_t off)
{
	struct msghdr msg = {};
	struct kvec iov = {};

	nvlist_t *nv = fnvlist_alloc();
	fnvlist_add_uint64(nv, VDEV_OBJECT_STORE_REQUEST, type);

	if (type != ZIO_TYPE_IOCTL) {
		fnvlist_add_uint64(nv, VDEV_OBJECT_STORE_SIZE, size);
		fnvlist_add_uint64(nv, VDEV_OBJECT_STORE_BLKID, off);
		fnvlist_add_uint8_array(nv, VDEV_OBJECT_STORE_DATA,
		    (uint8_t *)buf, size / sizeof (uint8_t));
	}

	size_t iov_size = 0;
	char *iov_buf = fnvlist_pack(nv, &iov_size);

	iov.iov_base = iov_buf;
	iov.iov_len = iov_size;
	iov_iter_kvec(&msg.msg_iter, WRITE, &iov, 1, size);

	int rc = sock_sendmsg(sock, &msg);
	if (rc != 0) {
		zfs_dbgmsg("zfs_object_store_pread failed: %d", rc);
		return (-rc);
	}

	fnvlist_free(nv);
	fnvlist_pack_free(iov_buf, iov_size);
	return (0);
}

static int
vdev_object_store_open(vdev_t *vd, uint64_t *psize, uint64_t *max_psize,
    uint64_t *logical_ashift, uint64_t *physical_ashift)
{
	vdev_object_store_t *vos;
	struct socket *sock;
	zfs_file_attr_t zfa;
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

	vos = vd->vdev_tsd = kmem_zalloc(sizeof (vdev_object_store_t), KM_SLEEP);

	error = zfs_object_store_open(vd->vdev_path,
	    vdev_object_store_open_mode(spa_mode(vd->vdev_spa)), &sock);
	if (error) {
		vd->vdev_stat.vs_aux = VDEV_AUX_OPEN_FAILED;
		return (error);
	}

	vos->vos_sock = sock;

#ifdef _KERNEL
	/*
	 * Make sure it's a socket file.
	 */
	if (zfs_file_getattr(sock->file, &zfa)) {
		return (SET_ERROR(ENODEV));
	}
	if (!S_ISSOCK(zfa.zfa_mode)) {
		vd->vdev_stat.vs_aux = VDEV_AUX_OPEN_FAILED;
		return (SET_ERROR(ENODEV));
	}
#endif

skip_open:

	*max_psize = *psize = UINT64_MAX; /* XXX Set to max for now */
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
	zio_t *zio = (zio_t *)arg;
	vdev_t *vd = zio->io_vd;
	vdev_object_store_t *vos = vd->vdev_tsd;
	void *buf;
	loff_t off;
	ssize_t size;
	int err;

	off = zio->io_offset; /*StorageBlockID */
	size = zio->io_size;

	if (zio->io_type == ZIO_TYPE_READ) {
		buf = abd_borrow_buf(zio->io_abd, zio->io_size);
		err = zfs_object_store_request(vos->vos_sock, zio->io_type, buf,
		    size, off);
		abd_return_buf_copy(zio->io_abd, buf, size);
	} else {
		buf = abd_borrow_buf_copy(zio->io_abd, zio->io_size);
		err = zfs_object_store_request(vos->vos_sock, zio->io_type, buf,
		    size, off);
		abd_return_buf_copy(zio->io_abd, buf, size);
		abd_return_buf(zio->io_abd, buf, size);
	}
	zio->io_error = err;
	if (zio->io_error == 0)
		zio->io_error = SET_ERROR(ENOSPC);

	zio_delay_interrupt(zio);
}

static void
vdev_object_store_io_start(zio_t *zio)
{
	vdev_t *vd = zio->io_vd;
	vdev_object_store_t *vos = vd->vdev_tsd;

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
			zio->io_error = zfs_object_store_request(vos->vos_sock,
			    zio->io_type, NULL, 0, 0);
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

vdev_ops_t vdev_object_storage_ops = {
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
	.vdev_op_type = VDEV_TYPE_OBJECT_STORE,		/* name of this vdev type */
	.vdev_op_leaf = B_TRUE			/* leaf vdev */
};

ZFS_MODULE_PARAM(zfs_vdev_object_store, vdev_object_store_,
	logical_ashift, ULONG, ZMOD_RW,
	"Logical ashift for object store based devices");
ZFS_MODULE_PARAM(zfs_vdev_object_store, vdev_object_store_,
	physical_ashift, ULONG, ZMOD_RW,
	"Physical ashift for object store based devices");
