# FileWriter in VFS

## Logic Design

A File is divide into chunks, and each chunk is divide into slices.
Each chunk has a fixed size, slice size is not fixed, but it cannot exceed
the chunk size. Each slice is a continuous piece of data, sequentially write
can extend the slice, for random write, we basically always need to create a
new slice. ~~In this way, we convert the random write to sequential write.~~

Actually, it still is random write in current implementation, no matter in
write-back cache or not, we need to create a new file or object for each slice
block. That introduce a lot of small files.

In Juice's implementation, they employee a background check to flush some files
to the remote for avoiding run out of inodes in the local file system. The
write-back cache has some optimization to do. Like merge multiple block file into
a big segment file like what levelDB does.

### Some Limitations

1. We need to commit each slice order by order


