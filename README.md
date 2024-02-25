# Kiseki

Kiseki is my learning rust project, a simple 'distributed' fuse file system that is port
of [JuiceFS](https://github.com/juicedata/juicefs).

**It's just a rust learning project and not ready for production.**

If you don't know juicefs very much, you can check
the [juicefs's architecture](https://juicefs.com/docs/en/overview/architecture.html).


# Quick Start

## Dependencies

FUSE must be installed to build or run programs that use fuse-rs (i.e. kernel driver and libraries. Some platforms may
also require userland utils like `fusermount`). A default installation of FUSE is usually sufficient.

To build fuse-rs or any program that depends on it, `pkg-config` needs to be installed as well.

### Linux

[FUSE for Linux][libfuse] is available in most Linux distributions and usually called `fuse`. To install on a Debian
based system:

```sh
sudo apt-get install fuse
```

Install on CentOS:

```sh
sudo yum install fuse
```

To build, FUSE libraries and headers are required. The package is usually called `libfuse-dev` or `fuse-devel`.
Also `pkg-config` is required for locating libraries and headers.

```sh
sudo apt-get install libfuse-dev pkg-config
```

```sh
sudo yum install fuse-devel pkgconfig
```

## Test mount

1. Start minio as backend storage.

```shell
just minio
```

2. Mount the file system to /tmp/kiseki automatically.

```shell
just release-mount
```

## Difference with juicefs

### 1. Write Buffer

JuiceFS uses a pre-allocated memory and growable bytes pool as write buffer,
but this pool is also used for make reading buffer.

Kiseki's write buffer pool is fixed-size, and it is consist of a in-memory bytes
pool and a mmap file.

### 2. Cache

JuiceFs use disk-eviction mechinism to manage the writeback cache,
in Kisekifs, it employs [moka](https://github.com/moka-rs/moka) to implement the
write back cache, much cleaner and efficient.

### 3. How to read slices

JuiceFs reorganize the slices into a linkedlist, kisekifs use [rangemap](https://github.com/jeffparsons/rangemap) to
handle the trick part.

# Bench

Just for reference, the benchmark is totally not accurate.

## Juicefs's bench

MetaEngine: local rocksdb;
ObjectStorage: local minio storage;
WriteBackCacheSize: 10G;
WriteBufferSize: 2G(1G memory + 1G mmap);

Cleaning kernel cache, may ask for root privilege...

BlockSize: 1 MiB, BigFileSize: 1024 MiB, SmallFileSize: 128 KiB, SmallFileCount: 100, NumThreads: 1

| ITEM             | VALUE           | COST         |
|------------------|-----------------|--------------|
| Write big file   | 2359.03 MiB/s   | 0.43 s/file  |
| Read big file    | 1139.28 MiB/s   | 0.90 s/file  |
| Write small file | 1001.5 files/s  | 1.00 ms/file |
| Read small file  | 6276.5 files/s  | 0.16 ms/file |
| Stat file        | 94324.9 files/s | 0.01 ms/file |

# Posix Compliance

Totally not compliant with posix, go to the issue page check the details.

# Why Kiseki?

Kiseki means miracle in Japanese, and I just pick it up from a Japanese
song while browsing the internet.

I am just interested in Rust and passionate about distributed systems.
Kiseki is my own exploration of building a file system in this language.

## Will you continue to develop it?

I don't know yet, the basic storage part is done, develop is fun and I have learned
a lot from it. So basically I have achieved my goal.
The rest of the work is endless posix compliance and performance optimization and
edge case handling, since i'm not employed by any company at present,
I have to focus on my job hunting and other stuff...
But who knows, we will see.

## Contribution

Very welcome, just open a PR or issue.

## Why JuiceFS?

While JuiceFS offers a powerful tool for data management,
porting it to Rust presents a unique opportunity to delve
into the inner workings of file systems and gain a deeper
understanding of their design and implementation.

## Disclaimer

Kiseki is an independent learning project
and is not endorsed by or affiliated with the Juice company.

# License

Apache-2.0

