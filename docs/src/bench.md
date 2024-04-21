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