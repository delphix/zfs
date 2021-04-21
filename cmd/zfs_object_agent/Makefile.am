sbin_PROGRAMS = target/@RUSTDIR@/zfs_object_agent

.PHONY: target/@RUSTDIR@/zfs_object_agent$(EXEEXT)

target/@RUSTDIR@/zfs_object_agent$(EXEEXT):
	cargo build $(RUSTFLAGS)

all: build

build:
	cargo build $(RUSTFLAGS)
