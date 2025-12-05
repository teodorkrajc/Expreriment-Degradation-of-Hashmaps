# Hashmap Experiment Codebase – System Instructions

This repository implements and benchmarks **four hashmap variants**:

- Linear Probing (LP)
- Robin Hood Hashing (RH)
- Chaining (CH)
- Cuckoo Hashing (CU)

All experiments use:

- **Fixed capacity: 2^20**
- **64-bit integer keys**
- **Simple value payload (int)**
- **A uniform 64-bit hash mixing function**
- **No rehashing or resizing** in Experiments 1 & 2

---

## 1. Hash Function

```c
uint64_t mix(uint64_t x) {
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9;
    x ^= x >> 27;
    x *= 0x94d049bb133111eb;
    x ^= x >> 31;
    return x;
}
```

### Indexing

- For LP/RH/CH:

```
index = mix(key) & (capacity - 1)
```

- For CU (two choices):

```
i1 = (uint32_t) mix(key) & (capacity - 1)
i2 = (uint32_t) (mix(key) >> 32) & (capacity - 1)
```

---

## 2. Hashmap Variant Rules

### Linear Probing
- Open addressing
- Deletion uses **tombstones**
- Track probe counts + tombstones

### Robin Hood
- Inserts may steal positions based on distance
- Use tombstones for delete
- Track probe lengths

### Chaining
- Array of buckets
- Track average/max chain length

### Cuckoo Hashing
- Two possible slots per key
- Kick & relocate on conflict
- Limit displacement attempts (100–500)
- Track displacement + failed inserts

---

## 3. Metrics

Every batch (e.g., 10k ops):
- Average latency of insert/lookup/delete
- Optional 95th percentile
- Variant-specific metrics:
  - LP/RH → probe lengths, tombstones
  - CH → average/max chain length
  - CU → displacement, failed inserts

### Suggested CSV schema

```
experiment,variant,batch,load_factor,
avg_insert,avg_lookup,avg_delete,
avg_probe,max_probe,
avg_chain,max_chain,
avg_displacement,max_displacement,
tombstones,failed_inserts
```

---

## 4. Experiment 1 – Load Factor vs Performance

1. Start empty
2. Insert keys until reaching:
   `{0.25, 0.5, 0.75, 0.85, 0.9, 0.95}`
3. At each LF measure:
   - avg insertion latency
   - avg lookup latency over random existing keys
   - variant metrics
4. If cuckoo insertion fails early, record LF and stop

---

## 5. Experiment 2 – Natural Degradation Under Churn

1. Pre-fill table to LF = 0.8
2. Perform 1M mixed ops:
   - 40% lookup
   - 30% insert
   - 30% delete
3. Keep set size constant:
   - delete random existing key
   - insert new key
4. Log metrics every batch (10k ops)
5. If variant fails, record batch index

---

## 6. Randomness / Seeds

- Use reproducible seeds
- Each run = global_seed + run_index
- Same seeds and workloads for all variants

---

## 7. Not Implemented Yet

Experiment 3 (rehashing / resizing) will be added later.
