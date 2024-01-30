# Kiseki

Kiseki is a Rust port of JuiceFS, a distributed file 
system known for its simplicity and efficiency.

## Why Kiseki?

Kiseki means miracle in Japanese, and I just pick it up from a Japanese 
song while browsing the internet. 

I am just interested in Rust and passionate about distributed systems.
Kiseki is my own exploration of building a file system in this language.

## Why JuiceFS?

While JuiceFS offers a powerful tool for data management, 
porting it to Rust presents a unique opportunity to delve
into the inner workings of file systems and gain a deeper
understanding of their design and implementation.

## Disclaimer 
Kiseki is an independent learning project 
and is not endorsed by or affiliated with the Juice company.

# Implementation
## Write
First we will receive an open request with write flag set to true.
Then we should create a file handle, which can hold some memory 
for the writing. 

When we receive a write request, we should write the data to the
file handle. Each write will make a slice, which cannot exceed the 
chunk size, and slice cannot cross the chunk boundary. Which means 
one chunk is made by one or more slices.

Under the hood, each slice will be divided into blocks, which is the 
actual data structure that will be written to the cache storage.

And if enable the cache, we should try to write the blocks into the
cache before the data has been uploaded, while when we upload the block
to object storage, we should take the ownership of the data. According 
to this, we make can sure the cache is always consistent with the backend
storage, since it is read only.

Write: 
    1. Find the file handle
    2. Find the file-local temporary write-only memory for the writing
        2.1 find the chunk by offset
        2.2 find the slice by offset and size
        2.3 slice is consist of blocks. (slice size: MIN_BLOCK_SIZE ~ MAX_CHUNK_SIZE)
        2.4 blocks is consist of pages. (block size: PAGE_SIZE ~ BLOCK_SIZE)
        2.5 MIN_BLOCK_SIZE = PAGE_SIZE = 64KiB = 1 << 16 bits
        2.6 BLOCK_SIZE = 4MiB = 1 << 22 bits
        2.7 MAX_CHUNK_SIZE = 64MiB = 1 << 26 bits
    3. Write the data to the memory
    4. Write data to the cache if necessary
    5. Write data to the backend storage if necessary

## Read
Read:
    1. Find the file handle
    2. Try to read data from cache
    3. If not found, read data from backend storage

## Concurrent Behavior
1. For reading, we don't use lock at all, even if there are someone is writing it.
2. For writing, we just lock the range.
