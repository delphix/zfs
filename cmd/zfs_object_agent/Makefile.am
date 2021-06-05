sbin_PROGRAMS = zfs_object_agent zoa_test

bin_SCRIPTS = scripts/zoa_chaos_monkey

zfs_object_agent_SOURCES = $(wildcard server/src/*.rs src/*.rs)

zoa_test_SOURCES = $(wildcard client/src/*.rs src/*.rs)

RUSTFLAGS = \
	-L $(abs_top_builddir)/lib/libzfs/.libs \
	-L $(abs_top_builddir)/lib/libnvpair/.libs

.PHONY: \
	zfs_object_agent$(EXEEXT) \
	zoa_test$(EXEEXT)

zfs_object_agent$(EXEEXT) :
	RUSTFLAGS="$(RUSTFLAGS)" cargo build --package zfs_object_agent $(RUSTTARGET)
	cp target/@RUSTDIR@/zfs_object_agent .

zoa_test$(EXEEXT) :
	RUSTFLAGS="$(RUSTFLAGS)" cargo build --package zoa_test $(RUSTTARGET)
	cp target/@RUSTDIR@/zoa_test .

all: build

build: \
	zfs_object_agent$(EXEEXT) \
	zoa_test$(EXEEXT)
