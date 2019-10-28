#!/bin/bash

read -r -d '' PROGRAM <<'EOF'
#include <sys/taskq.h>

kprobe:trace_zfs_taskq_ent__birth
{
        $tqent = (struct taskq_ent *)arg0;

        $tqent_id = $tqent->tqent_id;
        $tq_name = str($tqent->tqent_taskq->tq_name);

        @birth[$tq_name, $tqent_id] = nsecs;
}

kprobe:trace_zfs_taskq_ent__start
{
        $tqent = (struct taskq_ent *)arg0;

        @tqent_id[tid] = $tqent->tqent_id;
        @tq_name[tid] = str($tqent->tqent_taskq->tq_name);

        @start[@tq_name[tid], @tqent_id[tid]] = nsecs;
}

kprobe:trace_zfs_taskq_ent__finish
/ @birth[@tq_name[tid], @tqent_id[tid]] /
{
        @queue_lat_us[@tq_name[tid]] =
                hist((nsecs - @birth[@tq_name[tid], @tqent_id[tid]]));
        delete(@birth[@tq_name[tid], @tqent_id[tid]]);
}

kprobe:trace_zfs_taskq_ent__finish
/ @start[@tq_name[tid], @tqent_id[tid]] /
{
        $tqent = (struct taskq_ent *)arg0;

        @exec_lat_us[@tq_name[tid], ksym($tqent->tqent_func)] =
                hist((nsecs - @start[@tq_name[tid], @tqent_id[tid]]));
        delete(@start[@tq_name[tid], @tqent_id[tid]]);
}

kprobe:trace_zfs_taskq_ent__finish
{
        delete(@tq_name[tid]);
        delete(@tqent_id[tid]);
}

END
{
        clear(@birth);
        clear(@start);

        clear(@tq_name);
        clear(@tqent_id);
}
EOF

KVER=$(uname -r)
bpftrace \
        --include "/usr/src/zfs-$KVER/zfs_config.h" \
        -I "/usr/src/zfs-$KVER/include" \
        -I "/usr/src/zfs-$KVER/include/os/linux/kernel" \
        -I "/usr/src/zfs-$KVER/include/os/linux/spl" \
        -I "/usr/src/zfs-$KVER/include/os/linux/zfs" \
        -e "$PROGRAM"
