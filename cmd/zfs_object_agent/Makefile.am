sbin_PROGRAMS = target/@RUSTDIR@/zfs_object_agent \
	target/@RUSTDIR@/zoa_test

.PHONY: target/@RUSTDIR@/zfs_object_agent$(EXEEXT) \
	target/@RUSTDIR@/zoa_test$(EXEEXT)

target/@RUSTDIR@/zfs_object_agent$(EXEEXT):
	cargo build --workspace $(RUSTFLAGS)

target/@RUSTDIR@/zoa_test$(EXEEXT):
	cargo build --workspace $(RUSTFLAGS)

all: build

build:
	cargo build --workspace $(RUSTFLAGS)
