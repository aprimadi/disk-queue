# Disk Queue

FIFO queue backed by disk.

## Usage

TODO

## Limitations

The size of the item stored in the queue cannot be greater than 1024 bytes. 
This is due to the page size is set to 4096 bytes (4KB). If you need to store
items larger than 1024 bytes, consider forking this repository and change the 
page size.

