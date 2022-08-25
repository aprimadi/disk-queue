# Disk Queue

## Running Tests

Disk queue behavior is undefined if it's being opened by multiple process. 
Thus, run the tests using single thread:

```
cargo test -- --test-threads=1
```

