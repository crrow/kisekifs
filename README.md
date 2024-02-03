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
Each write is a slice, the slice size range from [64KiB - 4MiB].

Cache the slice into the cache, and write the slice into the object store.

## Read
According to the read range, get the slices.

If the file size less than the min slice size, do fast-path: just read the whole file, 
and cache it.

Slow path: check the current reading process, each if others is reading the range we wanted also,
read their data and build req for our self.

So will the following case happen ?

```
Question, do the following case exists? we need to make a SliceReader for just one bit.

insert slice-reader: 0..512
insert slice-reader: 520..720
insert slice-reader: 720..1021
insert slice-reader: 1021..1022
insert slice-reader: 1022..1023
invalidate 720 ~ 1021.
invalidate 1021 ~ 1022.

expect read: 1020..1024

find gap 1023..1024, need to make req
want: 1020..1024, overlapping but invalid range 720..1021, cut result: 1020..1021
want: 1020..1024, overlapping but invalid range 1021..1022, cut result: 1021..1022
after merge: want: 1020..1024, result: 1020..1022
after merge: want: 1020..1024, result: 1023..1024

```


## Concurrent Behavior
1. For reading, we don't use lock at all, even if there are someone is writing it.
2. For writing, we just lock the range.
