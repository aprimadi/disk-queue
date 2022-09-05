# Disk Queue

## Access Pattern

In the beginning, there are a lot more write requests than read requests but 
as items are added, there will be less and less item to be added. So toward 
the end, there will be more read requests.

## File Layout

We use a page size of 4KB (4096 bytes).

```
+--------------------------------------------------------+
| Metadata Page | Write Page | Page 1 | ... | Page N - 1 |
+--------------------------------------------------------+
```

The metadata page and write page is mmap-ed for efficient file write.

## Page Layout

### Metadata Page

```
+---------------------------+
| Magic (1 byte)            |
|---------------------------|
| Number of pages (8 bytes) |
|---------------------------|
| Number of items (8 bytes) |
|---------------------------|
| Read cursor (10 bytes)    |
|---------------------------|
| Write cursor (10 bytes)   |
|---------------------------|
| Empty space               |
+---------------------------+
```

A cursor is (page, slot) tuple which takes 8 + 2 bytes.

### Record Page

We call write page as well as page 1 to (page N - 1) as record page. Note 
that write page is actually page N (the tail page).

```
+---------------------------+ <- offset = 0
| Magic (1 byte)            | 
|---------------------------|
| Record 1 (variable sized) |
|---------------------------|
| ...                       |
|---------------------------|
| Record N (variable sized) |
|---------------------------|
| Empty space               |
|---------------------------|
| Offset, size N  (4 bytes) |
|---------------------------|
| ...                       |
|---------------------------|
| Offset, size 1  (4 bytes) |
|---------------------------|
| Num of records  (2 bytes) |
+---------------------------+

```

