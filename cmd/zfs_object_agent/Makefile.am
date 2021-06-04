lib_LTLIBRARIES = libzoa.la

sbin_PROGRAMS = zfs_object_agent zoa_test

zfs_object_agent_SOURCES = $(wildcard server/src/*.rs src/*.rs Cargo.lock Cargo.toml server/Cargo.toml)

zoa_test_SOURCES = $(wildcard client/src/*.rs src/*.rs Cargo.lock Cargo.toml client/Cargo.toml)

libzoa_la_SOURCES = $(wildcard zoa/src/*.rs src/*.rs)

RUSTFLAGS = \
	-L $(abs_top_builddir)/lib/libzfs/.libs \
	-L $(abs_top_builddir)/lib/libnvpair/.libs

.PHONY: \
	zfs_object_agent$(EXEEXT) \
	zoa_test$(EXEEXT) \
	libzoa.la

libzoa.la :
	RUSTFLAGS="$(RUSTFLAGS)" cargo build --package zoa $(RUSTTARGET)
	cp target/@RUSTDIR@/libzoa.so .

zfs_object_agent$(EXEEXT) :
	RUSTFLAGS="$(RUSTFLAGS)" cargo build --package zfs_object_agent $(RUSTTARGET)
	cp target/@RUSTDIR@/zfs_object_agent .

zoa_test$(EXEEXT) :
	RUSTFLAGS="$(RUSTFLAGS)" cargo build --package zoa_test $(RUSTTARGET)
	cp target/@RUSTDIR@/zoa_test .

all: build

build: \
	libzoa.la \
	zfs_object_agent$(EXEEXT) \
	zoa_test$(EXEEXT)
