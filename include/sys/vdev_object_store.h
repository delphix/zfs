/*
 * CDDL HEADER START
 *
 * This file and its contents are supplied under the terms of the
 * Common Development and Distribution License ("CDDL"), version 1.0.
 * You may only use this file in accordance with the terms of version
 * 1.0 of the CDDL.
 *
 * A full copy of the text of the CDDL should have accompanied this
 * source.  A copy of the CDDL is also available via the Internet at
 * http://www.illumos.org/license/CDDL.
 *
 * CDDL HEADER END
 */
/*
 * Copyright (c) 2021 by Delphix. All rights reserved.
 */

#include <sys/zfs_context.h>

typedef struct vdev_object_store_stats {
	uint64_t voss_blocks_count;
	uint64_t voss_blocks_bytes;
	uint64_t voss_pending_frees_count;
	uint64_t voss_pending_frees_bytes;
	uint64_t voss_objects_count;
} vdev_object_store_stats_t;

void object_store_begin_txg(vdev_t *, uint64_t);
void object_store_end_txg(vdev_t *, nvlist_t *, uint64_t);
void object_store_free_block(vdev_t *, uint64_t, uint64_t);
void object_store_flush_writes(spa_t *, uint64_t);
void object_store_restart_agent(vdev_t *vd);
void object_store_get_stats(vdev_t *, vdev_object_store_stats_t *);
