# FileWriter in VFS
## Logic Design
A File is divide into chunks, and each chunk is divide into slices.
Each chunk has a fixed size, slice size is not fixed, but it cannot exceed
the chunk size. Each slice is a continuous piece of data, sequentially write 
can extend the slice, for random write, we basically always need to create a
new slice. In this way, we convert the random write to sequential write.

### Some Limitations
1. We need to commit each slice order by order


