# LMDB Merge

Commands supported:
- Merge
- Count (keys)

## Merge

Merge `lmdb2` and `lmdb3` into `lmdb1` (create `lmdb1` if it doesn't exist).
Keys are overwritten.

```bash
cargo run --release -- merge -o /path/lmdb1 /path/lmdb2 /path/lmdb3
```

## Count 

```bash
cargo run --release -- count /path/lmdb1 /path /lmdb2 /path/lmdb3
```

Output:

```
"/path/lmdb1": 20282664 keys
"/path/lmdb2": 20282664 keys
"/path/lmdb3": 20282664 keys
Total: 60847992 keys
```

