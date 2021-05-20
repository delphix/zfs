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
#include <sys/metaslab_impl.h>
#include <sys/sock.h>

/*
 * Virtual device vector for object storage.
 */

/*
 * Possible keys in nvlist requests / responses to/from the Agent
 */
#define	AGENT_TYPE		"Type"
#define	AGENT_TYPE_CREATE_POOL		"create pool"
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
#define	AGENT_CREDENTIALS	"credentials"
#define	AGENT_ENDPOINT	"endpoint"
#define	AGENT_REGION	"region"
#define	AGENT_BLKID		"block"
#define	AGENT_DATA		"data"
#define	AGENT_REQUEST_ID	"request_id"
#define	AGENT_UBERBLOCK		"uberblock"
#define	AGENT_CONFIG		"config"
#define	AGENT_NEXT_BLOCK	"next_block"

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

#define	VOS_MAXREQ	1000

typedef enum {
	VOS_SOCK_CLOSED = (1 << 0),
	VOS_SOCK_SHUTTING_DOWN = (1 << 1),
	VOS_SOCK_SHUTDOWN = (1 << 2),
	VOS_SOCK_OPENING = (1 << 3),
	VOS_SOCK_OPEN = (1 << 4),
	VOS_SOCK_READY = (1 << 5)
} socket_state_t;


typedef struct vdev_object_store_request {
	uint64_t vosr_req;
	nvlist_t *vosr_nv;
} vdev_object_store_request_t;

typedef struct vdev_object_store {
	char *vos_endpoint;
	char *vos_region;
	char *vos_credential_location;
	char *vos_credentials;
	kthread_t *vos_agent_thread;
	kmutex_t vos_lock;
	kcondvar_t vos_cv;
	boolean_t vos_agent_thread_exit;

	kmutex_t vos_sock_lock;
	kcondvar_t vos_sock_cv;
	ksocket_t vos_sock;
	socket_state_t vos_sock_state;

	kmutex_t vos_outstanding_lock;
	kcondvar_t vos_outstanding_cv;
	zio_t *vos_outstanding_requests[VOS_MAXREQ];
	boolean_t vos_serial_done;
	boolean_t vos_send_txg;

	uint64_t vos_next_block;
	uberblock_t vos_uberblock;
	nvlist_t *vos_config;
} vdev_object_store_t;

static mode_t
vdev_object_store_open_mode(spa_mode_t spa_mode)
{
	mode_t mode = 0;

	if ((spa_mode & SPA_MODE_READ) && (spa_mode & SPA_MODE_WRITE)) {
		mode = O_RDWR;
	} else if (spa_mode & SPA_MODE_READ) {
		mode = O_RDONLY;
	} else {
		panic("unknown spa mode");
	}

	return (mode);
}

static struct sockaddr_un zfs_kernel_socket = {
	AF_UNIX, "/run/zfs_kernel_socket"
};

static inline vdev_object_store_request_t *
vdev_object_store_request_alloc(void)
{
	return (kmem_zalloc(
	    sizeof (vdev_object_store_request_t), KM_SLEEP));
}

static void
vdev_object_store_request_free(zio_t *zio)
{
	vdev_object_store_request_t *vosr = zio->io_vsd;
	fnvlist_free(vosr->vosr_nv);
}

static const zio_vsd_ops_t vdev_object_store_vsd_ops = {
	.vsd_free = vdev_object_store_request_free,
};

static void
zfs_object_store_wait(vdev_object_store_t *vos, socket_state_t state)
{
	ASSERT(MUTEX_HELD(&vos->vos_sock_lock));
	ASSERT(MUTEX_NOT_HELD(&vos->vos_outstanding_lock));
	while (vos->vos_sock_state < state) {
		cv_wait(&vos->vos_sock_cv, &vos->vos_sock_lock);
	}
}

static int
zfs_object_store_open(vdev_object_store_t *vos, char *bucket_name, int mode)
{
	ksocket_t s = INVALID_SOCKET;

	mutex_enter(&vos->vos_sock_lock);
	vos->vos_sock_state = VOS_SOCK_OPENING;
	int rc = ksock_create(PF_UNIX, SOCK_STREAM, 0, &s);
	if (rc != 0) {
		zfs_dbgmsg("zfs_object_store_open unable to create "
		    "socket: %d", rc);
		mutex_exit(&vos->vos_sock_lock);
		return (rc);
	}

	rc = ksock_connect(s, (struct sockaddr *)&zfs_kernel_socket,
	    sizeof (zfs_kernel_socket));
	if (rc != 0) {
		zfs_dbgmsg("zfs_object_store_open failed to "
		    "connect: %d", rc);
		ksock_close(s);
		s = INVALID_SOCKET;
	} else {
		zfs_dbgmsg("zfs_object_store_open, socket connection "
		    "ready, " SOCK_FMT, s);
	}

	VERIFY3P(vos->vos_sock, ==, INVALID_SOCKET);
	vos->vos_sock = s;
	zfs_dbgmsg("SOCKET OPEN(%px): " SOCK_FMT, curthread, vos->vos_sock);
	if (vos->vos_sock != INVALID_SOCKET) {
		vos->vos_sock_state = VOS_SOCK_OPEN;
		cv_broadcast(&vos->vos_sock_cv);
	}
	mutex_exit(&vos->vos_sock_lock);
	return (0);
}

static void
zfs_object_store_shutdown(vdev_object_store_t *vos)
{
	mutex_enter(&vos->vos_sock_lock);
	if (vos->vos_sock == INVALID_SOCKET) {
		mutex_exit(&vos->vos_sock_lock);
		return;
	}

	zfs_dbgmsg("SOCKET SHUTTING DOWN(%px): " SOCK_FMT, curthread,
	    vos->vos_sock);
	vos->vos_sock_state = VOS_SOCK_SHUTTING_DOWN;
	ksock_shutdown(vos->vos_sock, SHUT_RDWR);
	vos->vos_sock_state = VOS_SOCK_SHUTDOWN;
	mutex_exit(&vos->vos_sock_lock);
}

static void
zfs_object_store_close(vdev_object_store_t *vos)
{
	mutex_enter(&vos->vos_sock_lock);
	if (vos->vos_sock == INVALID_SOCKET) {
		mutex_exit(&vos->vos_sock_lock);
		return;
	}

	zfs_dbgmsg("SOCKET CLOSING(%px): " SOCK_FMT, curthread, vos->vos_sock);
	ksock_close(vos->vos_sock);
	vos->vos_sock = INVALID_SOCKET;
	vos->vos_sock_state = VOS_SOCK_CLOSED;
	mutex_exit(&vos->vos_sock_lock);
}

static int
agent_request(vdev_object_store_t *vos, nvlist_t *nv)
{
	ASSERT(MUTEX_HELD(&vos->vos_sock_lock));

	struct msghdr msg = {};
	kvec_t iov[2] = {};
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

	if (vos->vos_sock_state < VOS_SOCK_OPEN) {
		return (SET_ERROR(ENOTCONN));
	}

	size_t sent = ksock_send(vos->vos_sock, &msg, iov,
	    2, total_size);
	if (sent != total_size) {
		zfs_dbgmsg("sent wrong length to agent socket: "
		    "expected %d got %d",
		    (int)total_size, (int)sent);
	}

	fnvlist_pack_free(iov_buf, iov_size);
	return (sent == total_size ? 0 : SET_ERROR(EINTR));
}

/*
 * Send request to agent; returns request ID (index in
 * vos_outstanding_requests).  nvlist may be modified.
 */
static uint64_t
agent_request_zio(vdev_object_store_t *vos, zio_t *zio)
{
	ASSERT(MUTEX_HELD(&vos->vos_sock_lock));

	vdev_object_store_request_t *vosr = zio->io_vsd;
	uint64_t req;
	vdev_queue_t *vq = &zio->io_vd->vdev_queue;

	/*
	 * XXX need locking on requests array since this could be
	 * called concurrently
	 */
	mutex_enter(&vos->vos_outstanding_lock);
again:
	for (req = 0; req < VOS_MAXREQ; req++) {
		if (vos->vos_outstanding_requests[req] == NULL) {
			vos->vos_outstanding_requests[req] = zio;
			break;
		}
	}

	if (req == VOS_MAXREQ) {
		// put on vqc_queued_tree for vdev_get_stats_ex_impl()
		avl_add(&vq->vq_class[zio->io_priority].vqc_queued_tree, zio);
		/*
		 * Since we're going to sleep, we need to drop the
		 * vos_sock_lock so that others can make progress.
		 */
		mutex_exit(&vos->vos_sock_lock);
		// XXX we really shouldn't be blocking in vdev_op_io_start
		cv_wait(&vos->vos_outstanding_cv, &vos->vos_outstanding_lock);
		mutex_enter(&vos->vos_sock_lock);
		avl_remove(&vq->vq_class[zio->io_priority].vqc_queued_tree,
		    zio);

		goto again;
	}

	// for vdev_get_stats_ex_impl()
	vq->vq_class[zio->io_priority].vqc_active++;

	VERIFY3U(req, <, VOS_MAXREQ);
	fnvlist_add_uint64(vosr->vosr_nv, AGENT_REQUEST_ID, req);
	vosr->vosr_req = req;
	zfs_dbgmsg("agent_request_zio(req=%llu)",
	    req);
	mutex_exit(&vos->vos_outstanding_lock);

	agent_request(vos, vosr->vosr_nv);
	return (req);
}

static zio_t *
agent_complete_zio(vdev_object_store_t *vos, uint64_t req)
{
	mutex_enter(&vos->vos_outstanding_lock);
	VERIFY3U(req, <, VOS_MAXREQ);
	zio_t *zio = vos->vos_outstanding_requests[req];
	vdev_object_store_request_t *vosr = zio->io_vsd;
	VERIFY3U(vosr->vosr_req, ==, req);
	vos->vos_outstanding_requests[req] = NULL;
	cv_signal(&vos->vos_outstanding_cv);
	vdev_queue_t *vq = &zio->io_vd->vdev_queue;
	// for vdev_get_stats_ex_impl()
	vq->vq_class[zio->io_priority].vqc_active--;
	mutex_exit(&vos->vos_outstanding_lock);
	return (zio);
}

/*
 * Wait for a one-at-a-time operation to complete
 * (pool create, pool open, txg end).
 */
static void
agent_wait_serial(vdev_object_store_t *vos)
{
	mutex_enter(&vos->vos_outstanding_lock);
	while (!vos->vos_serial_done)
		cv_wait(&vos->vos_outstanding_cv, &vos->vos_outstanding_lock);
	vos->vos_serial_done = B_FALSE;
	mutex_exit(&vos->vos_outstanding_lock);
}

static void
agent_read_block(vdev_object_store_t *vos, zio_t *zio)
{
	/*
	 * We need to ensure that we only issue a request when the
	 * socket is ready. Otherwise, we block here since the agent
	 * might be in recovery.
	 */
	mutex_enter(&vos->vos_sock_lock);
	zfs_object_store_wait(vos, VOS_SOCK_READY);

	vdev_object_store_request_t *vosr = zio->io_vsd;
	uint64_t blockid = zio->io_offset >> 9;
	vosr->vosr_nv = fnvlist_alloc();
	fnvlist_add_string(vosr->vosr_nv, AGENT_TYPE, AGENT_TYPE_READ_BLOCK);
	fnvlist_add_uint64(vosr->vosr_nv, AGENT_SIZE, zio->io_size);
	fnvlist_add_uint64(vosr->vosr_nv, AGENT_BLKID, blockid);
	zfs_dbgmsg("agent_read_block(guid=%llu blkid=%llu)",
	    spa_guid(zio->io_spa), blockid);
	agent_request_zio(vos, zio);
	mutex_exit(&vos->vos_sock_lock);
}

static void
agent_free_block(vdev_object_store_t *vos, uint64_t offset, uint64_t asize)
{
	uint64_t blockid = offset >> 9;
	nvlist_t *nv = fnvlist_alloc();
	fnvlist_add_string(nv, AGENT_TYPE, AGENT_TYPE_FREE_BLOCK);
	fnvlist_add_uint64(nv, AGENT_BLKID, blockid);
	fnvlist_add_uint64(nv, AGENT_SIZE, asize);
	zfs_dbgmsg("agent_free_block(blkid=%llu, asize=%llu)",
	    blockid, asize);
	/*
	 * We need to ensure that we only issue a request when the
	 * socket is ready. Otherwise, we block here since the agent
	 * might be in recovery.
	 */
	mutex_enter(&vos->vos_sock_lock);
	zfs_object_store_wait(vos, VOS_SOCK_READY);
	agent_request(vos, nv);
	mutex_exit(&vos->vos_sock_lock);
	fnvlist_free(nv);
}

static void
agent_write_block(vdev_object_store_t *vos, zio_t *zio)
{
	/*
	 * We need to ensure that we only issue a request when the
	 * socket is ready. Otherwise, we block here since the agent
	 * might be in recovery.
	 */
	mutex_enter(&vos->vos_sock_lock);
	zfs_object_store_wait(vos, VOS_SOCK_READY);

	uint64_t blockid = zio->io_offset >> 9;
	vdev_object_store_request_t *vosr = zio->io_vsd;
	vosr->vosr_nv = fnvlist_alloc();
	fnvlist_add_string(vosr->vosr_nv, AGENT_TYPE, AGENT_TYPE_WRITE_BLOCK);
	fnvlist_add_uint64(vosr->vosr_nv, AGENT_BLKID, blockid);
	void *buf = abd_borrow_buf_copy(zio->io_abd, zio->io_size);
	fnvlist_add_uint8_array(vosr->vosr_nv, AGENT_DATA, buf, zio->io_size);
	abd_return_buf(zio->io_abd, buf, zio->io_size);
	zfs_dbgmsg("agent_write_block(guid=%llu blkid=%llu len=%llu)",
	    spa_guid(zio->io_spa),
	    blockid,
	    zio->io_size);
	agent_request_zio(vos, zio);
	mutex_exit(&vos->vos_sock_lock);
}

static void
agent_create_pool(vdev_t *vd, vdev_object_store_t *vos)
{
	/*
	 * We need to ensure that we only issue a request when the
	 * socket is ready. Otherwise, we block here since the agent
	 * might be in recovery.
	 */
	mutex_enter(&vos->vos_sock_lock);
	zfs_object_store_wait(vos, VOS_SOCK_OPEN);

	nvlist_t *nv = fnvlist_alloc();
	fnvlist_add_string(nv, AGENT_TYPE, AGENT_TYPE_CREATE_POOL);
	fnvlist_add_string(nv, AGENT_NAME, spa_name(vd->vdev_spa));
	fnvlist_add_uint64(nv, AGENT_GUID, spa_guid(vd->vdev_spa));
	fnvlist_add_string(nv, AGENT_CREDENTIALS, vos->vos_credentials);
	fnvlist_add_string(nv, AGENT_ENDPOINT, vos->vos_endpoint);
	fnvlist_add_string(nv, AGENT_REGION, vos->vos_region);
	fnvlist_add_string(nv, AGENT_BUCKET, vd->vdev_path);
	zfs_dbgmsg("agent_create_pool(guid=%llu name=%s bucket=%s)",
	    spa_guid(vd->vdev_spa),
	    spa_name(vd->vdev_spa),
	    vd->vdev_path);
	agent_request(vos, nv);
	mutex_exit(&vos->vos_sock_lock);
	fnvlist_free(nv);
	agent_wait_serial(vos);
}

static void
agent_open_pool(vdev_t *vd, vdev_object_store_t *vos)
{
	/*
	 * We need to ensure that we only issue a request when the
	 * socket is ready. Otherwise, we block here since the agent
	 * might be in recovery.
	 */
	mutex_enter(&vos->vos_sock_lock);
	zfs_object_store_wait(vos, VOS_SOCK_OPEN);

	nvlist_t *nv = fnvlist_alloc();
	fnvlist_add_string(nv, AGENT_TYPE, AGENT_TYPE_OPEN_POOL);
	fnvlist_add_uint64(nv, AGENT_GUID, spa_guid(vd->vdev_spa));
	fnvlist_add_string(nv, AGENT_CREDENTIALS, vos->vos_credentials);
	fnvlist_add_string(nv, AGENT_ENDPOINT, vos->vos_endpoint);
	fnvlist_add_string(nv, AGENT_REGION, vos->vos_region);
	fnvlist_add_string(nv, AGENT_BUCKET, vd->vdev_path);
	zfs_dbgmsg("agent_open_pool(guid=%llu bucket=%s)",
	    spa_guid(vd->vdev_spa),
	    vd->vdev_path);
	agent_request(vos, nv);
	mutex_exit(&vos->vos_sock_lock);
	fnvlist_free(nv);
	agent_wait_serial(vos);
}

static void
agent_begin_txg(vdev_object_store_t *vos, uint64_t txg)
{
	/*
	 * We need to ensure that we only issue a request when the
	 * socket is ready. Otherwise, we block here since the agent
	 * might be in recovery.
	 */
	mutex_enter(&vos->vos_sock_lock);
	zfs_object_store_wait(vos, VOS_SOCK_OPEN);

	nvlist_t *nv = fnvlist_alloc();
	fnvlist_add_string(nv, AGENT_TYPE, AGENT_TYPE_BEGIN_TXG);
	fnvlist_add_uint64(nv, AGENT_TXG, txg);
	zfs_dbgmsg("agent_begin_txg(%llu)",
	    txg);
	agent_request(vos, nv);
	mutex_exit(&vos->vos_sock_lock);
	fnvlist_free(nv);
}

static void
agent_end_txg(vdev_object_store_t *vos, uint64_t txg, void *ub_buf,
    size_t ub_len, void *config_buf, size_t config_len)
{
	/*
	 * We need to ensure that we only issue a request when the
	 * socket is ready. Otherwise, we block here since the agent
	 * might be in recovery.
	 */
	mutex_enter(&vos->vos_sock_lock);
	zfs_object_store_wait(vos, VOS_SOCK_READY);

	nvlist_t *nv = fnvlist_alloc();
	fnvlist_add_string(nv, AGENT_TYPE, AGENT_TYPE_END_TXG);
	fnvlist_add_uint64(nv, AGENT_TXG, txg);
	fnvlist_add_uint8_array(nv, AGENT_UBERBLOCK, ub_buf, ub_len);
	fnvlist_add_uint8_array(nv, AGENT_CONFIG, config_buf, config_len);
	zfs_dbgmsg("agent_end_txg(%llu)",
	    txg);
	agent_request(vos, nv);
	mutex_exit(&vos->vos_sock_lock);
	fnvlist_free(nv);
	agent_wait_serial(vos);
}

static void
agent_flush_writes(vdev_object_store_t *vos)
{
	mutex_enter(&vos->vos_sock_lock);
	zfs_object_store_wait(vos, VOS_SOCK_READY);

	nvlist_t *nv = fnvlist_alloc();
	fnvlist_add_string(nv, AGENT_TYPE, AGENT_TYPE_FLUSH_WRITES);
	zfs_dbgmsg("agent_flush");

	agent_request(vos, nv);
	mutex_exit(&vos->vos_sock_lock);
	fnvlist_free(nv);
}

static void
agent_reissue_zio(void *arg)
{
	vdev_t *vd = arg;
	vdev_object_store_t *vos = vd->vdev_tsd;

	zfs_dbgmsg("agent_reissue_zio running");

	/*
	 * Wait till the socket is opened.
	 */
	mutex_enter(&vos->vos_sock_lock);
	zfs_object_store_wait(vos, VOS_SOCK_OPEN);
	mutex_exit(&vos->vos_sock_lock);

	/*
	 * Re-establish the connection with the agent and send
	 * open/create message.
	 */
	if (vd->vdev_spa->spa_load_state == SPA_LOAD_CREATE) {
		agent_create_pool(vd, vos);
	}
	agent_open_pool(vd, vos);

	if (vos->vos_send_txg) {
		agent_begin_txg(vos, spa_syncing_txg(vd->vdev_spa));
	}

	mutex_enter(&vos->vos_sock_lock);
	mutex_enter(&vos->vos_outstanding_lock);
	for (uint64_t req = 0; req < VOS_MAXREQ; req++) {
		zio_t *zio = vos->vos_outstanding_requests[req];
		if (zio != NULL) {
			vdev_object_store_request_t *vosr = zio->io_vsd;
			VERIFY3U(vosr->vosr_req, ==, req);
			VERIFY3P(vosr->vosr_nv, !=, NULL);

			zfs_dbgmsg("ZIO REISSUE (%px) req %llu, blk %llu",
			    zio, vosr->vosr_req, zio->io_offset);
			if (agent_request(vos, vosr->vosr_nv) != 0) {
				zfs_dbgmsg("agent_reissue_zio failed");
				mutex_exit(&vos->vos_outstanding_lock);
				mutex_exit(&vos->vos_sock_lock);
				return;
			}
		}
	}
	mutex_exit(&vos->vos_outstanding_lock);

	/*
	 * Once we've reissued all pending I/Os, mark the socket
	 * as ready for use so that normal communication can
	 * continue.
	 */
	vos->vos_sock_state = VOS_SOCK_READY;
	cv_broadcast(&vos->vos_sock_cv);
	mutex_exit(&vos->vos_sock_lock);

	zfs_dbgmsg("agent_reissue_zio completed");
}


void
object_store_begin_txg(spa_t *spa, uint64_t txg)
{
	vdev_t *vd = spa->spa_root_vdev->vdev_child[0];
	ASSERT3P(vd->vdev_ops, ==, &vdev_object_store_ops);
	vdev_object_store_t *vos = vd->vdev_tsd;
	agent_begin_txg(vos, txg);
	vos->vos_send_txg = B_TRUE;
}

void
object_store_end_txg(spa_t *spa, nvlist_t *config, uint64_t txg)
{
	vdev_t *vd = spa->spa_root_vdev->vdev_child[0];
	ASSERT3P(vd->vdev_ops, ==, &vdev_object_store_ops);
	vdev_object_store_t *vos = vd->vdev_tsd;
	size_t nvlen;
	char *nvbuf = fnvlist_pack(config, &nvlen);
	agent_end_txg(vos, txg,
	    &spa->spa_uberblock, sizeof (spa->spa_uberblock), nvbuf, nvlen);
	fnvlist_pack_free(nvbuf, nvlen);

	if (vos->vos_config != NULL)
		fnvlist_free(vos->vos_config);
	vos->vos_config = fnvlist_dup(config);
	vos->vos_send_txg = B_FALSE;
}

void
object_store_free_block(vdev_t *vd, uint64_t offset, uint64_t asize)
{
	ASSERT3P(vd->vdev_ops, ==, &vdev_object_store_ops);
	vdev_object_store_t *vos = vd->vdev_tsd;
	agent_free_block(vos, offset, asize);
}

void
object_store_flush_writes(spa_t *spa)
{
	vdev_t *vd = spa->spa_root_vdev->vdev_child[0];
	ASSERT3P(vd->vdev_ops, ==, &vdev_object_store_ops);
	vdev_object_store_t *vos = vd->vdev_tsd;
	agent_flush_writes(vos);
}

static int
agent_read_all(vdev_object_store_t *vos, void *buf, size_t len)
{
	size_t recvd_total = 0;
	while (recvd_total < len) {
		struct msghdr msg = {};
		kvec_t iov = {};

		iov.iov_base = buf + recvd_total;
		iov.iov_len = len - recvd_total;

		mutex_enter(&vos->vos_lock);
		if (vos->vos_agent_thread_exit) {
			zfs_dbgmsg("(%px) agent_read_all shutting down",
			    curthread);
			mutex_exit(&vos->vos_lock);
			return (SET_ERROR(ENOTCONN));
		}
		mutex_exit(&vos->vos_lock);

		size_t recvd = ksock_receive(vos->vos_sock,
		    &msg, &iov, 1, len - recvd_total, 0);
		if (recvd > 0) {
			recvd_total += recvd;
			if (recvd_total < len) {
				zfs_dbgmsg("incomplete recvmsg but trying for "
				    "more len=%d recvd=%d recvd_total=%d",
				    (int)len,
				    (int)recvd,
				    (int)recvd_total);
			}
		} else {
			zfs_dbgmsg("got wrong length from agent socket: "
			    "for total size %d, already received %d, "
			    "expected up to %d got %d",
			    (int)len,
			    (int)recvd_total,
			    (int)(len - recvd_total),
			    (int)recvd);
			/* XXX - Do we need to check for errors too? */
			if (recvd == 0)
				return (SET_ERROR(EAGAIN));
		}
	}
	return (0);
}

static int
agent_reader(void *arg)
{
	vdev_object_store_t *vos = arg;
	uint64_t nvlist_len = 0;
	int err = agent_read_all(vos, &nvlist_len, sizeof (nvlist_len));
	if (err != 0) {
		zfs_dbgmsg("agent_reader(%px) got err %d", curthread, err);
		return (err);
	}

	void *buf = kmem_alloc(nvlist_len, KM_SLEEP);
	err = agent_read_all(vos, buf, nvlist_len);
	if (err != 0) {
		zfs_dbgmsg("2 agent_reader(%px) got err %d", curthread, err);
		kmem_free(buf, nvlist_len);
		return (err);
	}

	nvlist_t *nv;
	err = nvlist_unpack(buf, nvlist_len, &nv, KM_SLEEP);
	if (err != 0) {
		zfs_dbgmsg("got error %d from nvlist_unpack(len=%d)",
		    err, (int)nvlist_len);
		return (EAGAIN);
	}
	// nvlist_t *nv = fnvlist_unpack(buf, nvlist_len);
	kmem_free(buf, nvlist_len);

	const char *type = fnvlist_lookup_string(nv, AGENT_TYPE);
	zfs_dbgmsg("got response from agent type=%s", type);
	// XXX debug message the nvlist
	if (strcmp(type, "pool create done") == 0 ||
	    strcmp(type, "end txg done") == 0) {
		mutex_enter(&vos->vos_outstanding_lock);
		ASSERT(!vos->vos_serial_done);
		vos->vos_serial_done = B_TRUE;
		cv_broadcast(&vos->vos_outstanding_cv);
		mutex_exit(&vos->vos_outstanding_lock);
	} else if (strcmp(type, "pool open done") == 0) {
		uint_t len;
		uint8_t *arr;
		int err = nvlist_lookup_uint8_array(nv, AGENT_UBERBLOCK,
		    &arr, &len);
		if (err == 0) {
			ASSERT3U(len, ==, sizeof (uberblock_t));
			bcopy(arr, &vos->vos_uberblock, len);
			VERIFY0(nvlist_lookup_uint8_array(nv,
			    AGENT_CONFIG, &arr, &len));
			vos->vos_config = fnvlist_unpack((char *)arr, len);
		}

		uint64_t next_block = fnvlist_lookup_uint64(nv,
		    AGENT_NEXT_BLOCK);
		vos->vos_next_block = next_block;

		zfs_dbgmsg("got pool open done len=%llu block=%llu",
		    len, next_block);

		fnvlist_free(nv);
		mutex_enter(&vos->vos_outstanding_lock);
		ASSERT(!vos->vos_serial_done);
		vos->vos_serial_done = B_TRUE;
		cv_broadcast(&vos->vos_outstanding_cv);
		mutex_exit(&vos->vos_outstanding_lock);
	} else if (strcmp(type, "read done") == 0) {
		uint64_t req = fnvlist_lookup_uint64(nv,
		    AGENT_REQUEST_ID);
		uint_t len;
		void *data = fnvlist_lookup_uint8_array(nv,
		    AGENT_DATA, &len);
		zfs_dbgmsg("got read done req=%llu datalen=%u",
		    req, len);
		VERIFY3U(req, <, VOS_MAXREQ);
		zio_t *zio = agent_complete_zio(vos, req);
		VERIFY3U(fnvlist_lookup_uint64(nv, AGENT_BLKID), ==,
		    zio->io_offset >> 9);
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
		zio_t *zio = agent_complete_zio(vos, req);
		VERIFY3U(fnvlist_lookup_uint64(nv, AGENT_BLKID), ==,
		    zio->io_offset >> 9);
		fnvlist_free(nv);
		zio_delay_interrupt(zio);
	} else {
		zfs_dbgmsg("unrecognized response type!");
	}
	return (0);
}

static int
vdev_object_store_socket_open(vdev_t *vd)
{
	vdev_object_store_t *vos = vd->vdev_tsd;

	/*
	 * XXX - We open the socket continuously waiting
	 * for the agent to start accepting connections.
	 * We may need to provide a mechanism to break out and
	 * fail the import instead.
	 */
	while (!vos->vos_agent_thread_exit &&
	    vos->vos_sock == INVALID_SOCKET) {

		mutex_enter(&vos->vos_lock);
		VERIFY3P(vos->vos_sock, ==, NULL);

		int error = zfs_object_store_open(vos, vd->vdev_path,
		    vdev_object_store_open_mode(
		    spa_mode(vd->vdev_spa)));
		if (error != 0) {
			mutex_exit(&vos->vos_lock);
			return (error);
		}

		if (vos->vos_sock == INVALID_SOCKET) {
			delay(hz);
		} else {
			cv_broadcast(&vos->vos_cv);
		}

		mutex_exit(&vos->vos_lock);
	}
	return (0);
}

static void
vdev_agent_thread(void *arg)
{
	vdev_t *vd = arg;
	vdev_object_store_t *vos = vd->vdev_tsd;

	while (!vos->vos_agent_thread_exit) {

		int err = agent_reader(vos);
		if (vos->vos_agent_thread_exit || err == 0)
			continue;


		/*
		 * The agent has crashed so we need to start recovery.
		 * We first need to shutdown the socket. Manipulating
		 * the socket requires consumers to hold the vosr_sock_lock
		 * which also protects the vosr_sock_state.
		 *
		 * Once the socket is shutdown, no other thread should
		 * be able to send or receive on that socket. We also need
		 * to wakeup any threads that are currently waiting for a
		 * serial request.
		 */


		ASSERT3U(vd->vdev_state, ==, VDEV_STATE_HEALTHY);
		ASSERT3U(err, ==, EAGAIN);
		zfs_dbgmsg("(%px) agent_reader exited, reopen", curthread);

		zfs_object_store_shutdown(vos);
		VERIFY3U(vos->vos_sock_state, ==, VOS_SOCK_SHUTDOWN);

		/*
		 * Wakeup any threads that are waiting for a
		 * response.
		 */
		mutex_enter(&vos->vos_outstanding_lock);
		vos->vos_serial_done = B_TRUE;
		cv_broadcast(&vos->vos_outstanding_cv);
		mutex_exit(&vos->vos_outstanding_lock);

		/*
		 * XXX - it's possible that the socket may reopen
		 * immediately because the connection is not completely
		 * closed by the server. To prevent this, we delay here.
		 */
		delay(hz);

		zfs_object_store_close(vos);
		ASSERT3P(vos->vos_sock, ==, INVALID_SOCKET);
		VERIFY3U(vos->vos_sock_state, ==, VOS_SOCK_CLOSED);

		vdev_object_store_socket_open(vd);
		zfs_dbgmsg("REOPENED(%px) sock " SOCK_FMT, curthread,
		    vos->vos_sock);

		/* XXX - make sure we only run this once and it completes */
		VERIFY3U(taskq_dispatch(system_taskq,
		    agent_reissue_zio, vd, TQ_SLEEP), !=, TASKQID_INVALID);
	}

	mutex_enter(&vos->vos_lock);
	vos->vos_agent_thread = NULL;
	cv_broadcast(&vos->vos_cv);
	mutex_exit(&vos->vos_lock);
	zfs_dbgmsg("agent thread exited");
	thread_exit();
}

static int
vdev_object_store_init(spa_t *spa, nvlist_t *nv, void **tsd)
{
	vdev_object_store_t *vos;
	char *val = NULL;

	vos = *tsd = kmem_zalloc(sizeof (vdev_object_store_t), KM_SLEEP);
	vos->vos_sock = INVALID_SOCKET;
	mutex_init(&vos->vos_lock, NULL, MUTEX_DEFAULT, NULL);
	mutex_init(&vos->vos_sock_lock, NULL, MUTEX_DEFAULT, NULL);
	mutex_init(&vos->vos_outstanding_lock, NULL, MUTEX_DEFAULT, NULL);
	cv_init(&vos->vos_cv, NULL, CV_DEFAULT, NULL);
	cv_init(&vos->vos_sock_cv, NULL, CV_DEFAULT, NULL);
	cv_init(&vos->vos_outstanding_cv, NULL, CV_DEFAULT, NULL);

	if (!nvlist_lookup_string(nv,
	    zpool_prop_to_name(ZPOOL_PROP_OBJ_ENDPOINT), &val)) {
		vos->vos_endpoint = kmem_strdup(val);
	} else {
		return (SET_ERROR(EINVAL));
	}
	if (!nvlist_lookup_string(nv,
	    zpool_prop_to_name(ZPOOL_PROP_OBJ_REGION), &val)) {
		vos->vos_region = kmem_strdup(val);
	} else {
		return (SET_ERROR(EINVAL));
	}
	if (!nvlist_lookup_string(nv,
	    zpool_prop_to_name(ZPOOL_PROP_OBJ_CREDENTIALS), &val)) {
		vos->vos_credential_location = kmem_strdup(val);
	} else {
		return (SET_ERROR(EINVAL));
	}
	if (!nvlist_lookup_string(nv,
	    ZPOOL_CONFIG_OBJSTORE_CREDENTIALS, &val)) {
		vos->vos_credentials = kmem_strdup(val);
	}

	zfs_dbgmsg("vdev_object_store_init, endpoint=%s region=%s cred=%s",
	    vos->vos_endpoint, vos->vos_region, vos->vos_credentials);

	return (0);
}

static void
vdev_object_store_fini(vdev_t *vd)
{
	vdev_object_store_t *vos = vd->vdev_tsd;

	mutex_destroy(&vos->vos_lock);
	mutex_destroy(&vos->vos_sock_lock);
	mutex_destroy(&vos->vos_outstanding_lock);
	cv_destroy(&vos->vos_cv);
	cv_destroy(&vos->vos_sock_cv);
	cv_destroy(&vos->vos_outstanding_cv);
	if (vos->vos_endpoint != NULL) {
		kmem_strfree(vos->vos_endpoint);
	}
	if (vos->vos_region != NULL) {
		kmem_strfree(vos->vos_region);
	}
	if (vos->vos_credential_location != NULL) {
		kmem_strfree(vos->vos_credential_location);
	}
	if (vos->vos_credentials != NULL) {
		kmem_strfree(vos->vos_credentials);
	}
	if (vos->vos_config != NULL) {
		fnvlist_free(vos->vos_config);
	}
	kmem_free(vd->vdev_tsd, sizeof (vdev_object_store_t));
	vd->vdev_tsd = NULL;

	zfs_dbgmsg("vdev_object_store_fini");
}


static int
vdev_object_store_open(vdev_t *vd, uint64_t *psize, uint64_t *max_psize,
    uint64_t *logical_ashift, uint64_t *physical_ashift)
{
	vdev_object_store_t *vos;
	int error = 0;

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

	vos = vd->vdev_tsd;

	/*
	 * Reopen the device if it's not currently open.  Otherwise,
	 * just update the physical size of the device.
	 */
	if (vd->vdev_reopening) {
		goto skip_open;
	}
	ASSERT(vd->vdev_path != NULL);
	ASSERT3P(vos->vos_agent_thread, ==, NULL);

	error = vdev_object_store_socket_open(vd);

	/* XXX - this can't happen today */
	if (error) {
		vd->vdev_stat.vs_aux = VDEV_AUX_OPEN_FAILED;
		return (error);
	}

	vos->vos_agent_thread = thread_create(NULL, 0, vdev_agent_thread,
	    vd, 0, &p0, TS_RUN, defclsyspri);

	if (vd->vdev_spa->spa_load_state == SPA_LOAD_CREATE) {
		agent_create_pool(vd, vos);
	}
	agent_open_pool(vd, vos);

	/*
	 * Socket is now ready for communication, wake up
	 * anyone waiting.
	 */
	mutex_enter(&vos->vos_sock_lock);
	vos->vos_sock_state = VOS_SOCK_READY;
	cv_broadcast(&vos->vos_sock_cv);
	mutex_exit(&vos->vos_sock_lock);

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

	mutex_enter(&vos->vos_lock);
	vos->vos_agent_thread_exit = B_TRUE;
	zfs_object_store_shutdown(vos);
	mutex_exit(&vos->vos_lock);

	mutex_enter(&vos->vos_lock);
	while (vos->vos_agent_thread != NULL) {
		zfs_dbgmsg("vdev_object_store_close: shutting down agent");
		cv_wait(&vos->vos_cv, &vos->vos_lock);
	}
	zfs_object_store_close(vos);
	mutex_exit(&vos->vos_lock);
	ASSERT3P(vos->vos_sock, ==, INVALID_SOCKET);
	vd->vdev_delayed_close = B_FALSE;
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

	zio->io_vsd = vdev_object_store_request_alloc();
	zio->io_vsd_ops = &vdev_object_store_vsd_ops;
	zio->io_target_timestamp = zio_handle_io_delay(zio);
	vdev_object_store_io_strategy(zio);
}

/* ARGSUSED */
static void
vdev_object_store_io_done(zio_t *zio)
{
}

static void
vdev_object_store_config_generate(vdev_t *vd, nvlist_t *nv)
{
	vdev_object_store_t *vos = vd->vdev_tsd;

	fnvlist_add_string(nv,
	    zpool_prop_to_name(ZPOOL_PROP_OBJ_CREDENTIALS),
	    vos->vos_credential_location);
	fnvlist_add_string(nv, ZPOOL_CONFIG_OBJSTORE_CREDENTIALS,
	    vos->vos_credentials);
	fnvlist_add_string(nv,
	    zpool_prop_to_name(ZPOOL_PROP_OBJ_ENDPOINT), vos->vos_endpoint);
	fnvlist_add_string(nv,
	    zpool_prop_to_name(ZPOOL_PROP_OBJ_REGION), vos->vos_region);
}

static void
vdev_object_store_metaslab_init(vdev_t *vd, metaslab_t *msp, uint64_t *ms_start,
    uint64_t *ms_size)
{
	vdev_object_store_t *vos = vd->vdev_tsd;
	msp->ms_lbas[0] = vos->vos_next_block;
}

uberblock_t *
vdev_object_store_get_uberblock(vdev_t *vd)
{
	vdev_object_store_t *vos = vd->vdev_tsd;
	return (&vos->vos_uberblock);
}

nvlist_t *
vdev_object_store_get_config(vdev_t *vd)
{
	vdev_object_store_t *vos = vd->vdev_tsd;
	return (fnvlist_dup(vos->vos_config));
}

vdev_ops_t vdev_object_store_ops = {
	.vdev_op_init = vdev_object_store_init,
	.vdev_op_fini = vdev_object_store_fini,
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
	.vdev_op_metaslab_init = vdev_object_store_metaslab_init,
	.vdev_op_config_generate = vdev_object_store_config_generate,
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
