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
## Backend of Meta Data
At present, Rocksdb is chosen as the backend of meta data for developing and test purpose.
In the future, it will be replaced by a more sophisticated and distributed database.

## Object Storage
Choose this [ObjectStorage](https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html#tymethod.put_multipart) 
rather than [OpenDal]() since the later doesn't support multipart upload or append write operation. 