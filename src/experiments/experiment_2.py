"""
Experiment 2: Natural Degradation Under Churn

Measures how hashmap performance degrades over time with mixed operations
(inserts, lookups, deletes) while maintaining constant table size at LF=0.8.
"""

import random
import time
import csv
from typing import List, Dict, Any
from hashmaps import CAPACITY, LinearProbingMap, RobinHoodMap, ChainingMap, CuckooMap


# Hashmap variants to test
VARIANTS = [
    ('LP', LinearProbingMap),
    ('RH', RobinHoodMap),
    ('CH', ChainingMap),
    ('CU', CuckooMap),
]

# Initial load factor (pre-fill)
INITIAL_LOAD_FACTOR = 0.8

# Total mixed operations to perform
TOTAL_OPERATIONS = 1_000_000

# Operation distribution
LOOKUP_RATIO = 0.4  # 40%
INSERT_RATIO = 0.3  # 30%
DELETE_RATIO = 0.3  # 30%

# Batch size for metrics collection
BATCH_SIZE = 10_000

# Seeds for reproducibility
SEEDS = [42]

# Seed offsets for different operation types
PREFILL_SEED_OFFSET = 0
CHURN_SEED_OFFSET = 2000
LOOKUP_SEED_OFFSET = 3000


def generate_keys(count: int, seed: int) -> List[int]:
    """
    Generate random 64-bit integer keys.
    
    Args:
        count: Number of keys to generate
        seed: Random seed for reproducibility
        
    Returns:
        List of random keys
    """
    rng = random.Random(seed)
    keys = set()
    while len(keys) < count:
        key = rng.randint(0, 2**64 - 1)
        keys.add(key)
    return list(keys)


def prefill_hashmap(hashmap: Any, keys: List[int]) -> bool:
    """
    Pre-fill hashmap to initial load factor.
    
    Args:
        hashmap: The hashmap instance
        keys: Pre-generated keys to insert
        
    Returns:
        True if successful, False if failed
    """
    target_size = int(CAPACITY * INITIAL_LOAD_FACTOR)
    
    for i in range(target_size):
        result = hashmap.insert(keys[i], i)
        if not result.success:
            return False
    
    return True


def perform_batch_operations(
    hashmap: Any,
    existing_keys: List[int],
    new_keys: List[int],
    new_key_index: int,
    batch_num: int,
    seed: int
) -> tuple[float, float, float, int, bool]:
    """
    Perform a batch of mixed operations and measure latencies.
    
    Args:
        hashmap: The hashmap instance
        existing_keys: List of keys currently in the hashmap
        new_keys: Pool of new keys to insert
        new_key_index: Current index in new_keys pool
        batch_num: Current batch number
        seed: Base seed for this batch
        
    Returns:
        Tuple of (avg_lookup_ns, avg_insert_ns, avg_delete_ns, new_key_index, success)
    """
    rng = random.Random(seed + batch_num)
    
    # Calculate operation counts
    num_lookups = int(BATCH_SIZE * LOOKUP_RATIO)
    num_inserts = int(BATCH_SIZE * INSERT_RATIO)
    num_deletes = int(BATCH_SIZE * DELETE_RATIO)
    
    # Adjust to ensure exact batch size
    num_lookups += BATCH_SIZE - (num_lookups + num_inserts + num_deletes)
    
    lookup_time = 0.0
    insert_time = 0.0
    delete_time = 0.0
    
    # Create operation sequence and shuffle for realistic interleaving
    operations = ['lookup'] * num_lookups + ['insert'] * num_inserts + ['delete'] * num_deletes
    rng.shuffle(operations)
    
    for op in operations:
        if op == 'lookup':
            # Lookup random existing key
            key = rng.choice(existing_keys)
            start = time.perf_counter()
            result, value = hashmap.lookup(key)
            lookup_time += time.perf_counter() - start
            
        elif op == 'delete':
            # Delete random existing key
            key_idx = rng.randrange(len(existing_keys))
            key = existing_keys[key_idx]
            start = time.perf_counter()
            result = hashmap.delete(key)
            delete_time += time.perf_counter() - start
            
            if result.success:
                # Remove from existing keys
                existing_keys[key_idx] = existing_keys[-1]
                existing_keys.pop()
            
        elif op == 'insert':
            # Insert new key
            if new_key_index >= len(new_keys):
                # Ran out of new keys
                return (
                    (lookup_time / num_lookups * 1e9) if num_lookups > 0 else 0.0,
                    (insert_time / num_inserts * 1e9) if num_inserts > 0 else 0.0,
                    (delete_time / num_deletes * 1e9) if num_deletes > 0 else 0.0,
                    new_key_index,
                    False
                )
            
            key = new_keys[new_key_index]
            new_key_index += 1
            
            start = time.perf_counter()
            result = hashmap.insert(key, new_key_index)
            insert_time += time.perf_counter() - start
            
            if not result.success:
                # Insertion failed
                return (
                    (lookup_time / num_lookups * 1e9) if num_lookups > 0 else 0.0,
                    (insert_time / num_inserts * 1e9) if num_inserts > 0 else 0.0,
                    (delete_time / num_deletes * 1e9) if num_deletes > 0 else 0.0,
                    new_key_index,
                    False
                )
            
            # Add to existing keys
            existing_keys.append(key)
    
    # Convert to nanoseconds
    avg_lookup_ns = (lookup_time / num_lookups * 1e9) if num_lookups > 0 else 0.0
    avg_insert_ns = (insert_time / num_inserts * 1e9) if num_inserts > 0 else 0.0
    avg_delete_ns = (delete_time / num_deletes * 1e9) if num_deletes > 0 else 0.0
    
    return avg_lookup_ns, avg_insert_ns, avg_delete_ns, new_key_index, True


def collect_metrics(hashmap: Any, variant: str) -> dict:
    """
    Collect variant-specific metrics.
    
    Args:
        hashmap: The hashmap instance
        variant: Variant name (LP, RH, CH, CU)
        
    Returns:
        Dictionary of metrics
    """
    metrics = {
        'load_factor': hashmap.load_factor(),
        'size': hashmap.size,
    }
    
    if variant in ['LP', 'RH']:
        metrics['tombstones'] = hashmap.tombstone_count()
        if variant == 'RH':
            metrics['avg_probe'] = hashmap.get_avg_probe_length()
            metrics['max_probe'] = hashmap.get_max_probe_length()
    
    elif variant == 'CH':
        metrics['avg_chain'] = hashmap.get_avg_chain_length()
        metrics['max_chain'] = hashmap.get_max_chain_length()
    
    elif variant == 'CU':
        metrics['failed_inserts'] = hashmap.get_failed_inserts()
    
    return metrics


def run_experiment_for_variant(
    variant_name: str,
    variant_class: Any,
    prefill_keys: List[int],
    churn_keys: List[int],
    seed: int
) -> List[dict]:
    """
    Run experiment 2 for a single hashmap variant.
    
    Args:
        variant_name: Name of variant (LP, RH, CH, CU)
        variant_class: Class of the hashmap
        prefill_keys: Keys for initial population
        churn_keys: Keys for churn operations
        seed: Seed for this run
        
    Returns:
        List of result dictionaries (one per batch)
    """
    print(f"  Running {variant_name}...")
    
    hashmap = variant_class()
    results = []
    
    # Pre-fill to initial load factor
    print(f"    Pre-filling to LF={INITIAL_LOAD_FACTOR}...")
    if not prefill_hashmap(hashmap, prefill_keys):
        print(f"    {variant_name} failed during pre-fill")
        return results
    
    # Track existing keys
    existing_keys = prefill_keys[:hashmap.size].copy()
    
    # Perform batches of mixed operations
    num_batches = TOTAL_OPERATIONS // BATCH_SIZE
    new_key_index = 0
    
    for batch_num in range(num_batches):
        avg_lookup_ns, avg_insert_ns, avg_delete_ns, new_key_index, success = perform_batch_operations(
            hashmap, existing_keys, churn_keys, new_key_index, batch_num, seed + CHURN_SEED_OFFSET
        )
        
        if not success:
            print(f"    {variant_name} failed at batch {batch_num + 1}/{num_batches}")
            break
        
        # Collect metrics
        metrics = collect_metrics(hashmap, variant_name)
        
        # Record results
        result = {
            'experiment': 'exp2',
            'variant': variant_name,
            'seed': seed,
            'batch': batch_num + 1,
            'operations': (batch_num + 1) * BATCH_SIZE,
            'load_factor': metrics['load_factor'],
            'size': metrics['size'],
            'avg_lookup_ns': avg_lookup_ns,
            'avg_insert_ns': avg_insert_ns,
            'avg_delete_ns': avg_delete_ns,
            'tombstones': metrics.get('tombstones', 0),
            'avg_probe': metrics.get('avg_probe', 0.0),
            'max_probe': metrics.get('max_probe', 0),
            'avg_chain': metrics.get('avg_chain', 0.0),
            'max_chain': metrics.get('max_chain', 0),
            'failed_inserts': metrics.get('failed_inserts', 0),
        }
        results.append(result)
        
        if (batch_num + 1) % 10 == 0:
            print(f"    Batch {batch_num + 1}/{num_batches}: "
                  f"lookup={avg_lookup_ns:.1f}ns, insert={avg_insert_ns:.1f}ns, delete={avg_delete_ns:.1f}ns")
    
    print(f"    Completed {len(results)} batches")
    return results


def run_experiment_2(output_file: str = 'results/experiment_2_results.csv'):
    """
    Run Experiment 2 for all variants and all seeds.
    
    Args:
        output_file: Path to output CSV file (relative to project root)
    """
    print("=" * 60)
    print("EXPERIMENT 2: Natural Degradation Under Churn")
    print("=" * 60)
    
    all_results = []
    
    for seed in SEEDS:
        print(f"\nRun with seed={seed}")
        print("-" * 60)
        
        # Pre-generate keys for pre-fill phase
        prefill_size = int(CAPACITY * INITIAL_LOAD_FACTOR)
        print(f"Generating {prefill_size:,} keys for pre-fill...")
        prefill_keys = generate_keys(prefill_size, seed + PREFILL_SEED_OFFSET)
        
        # Pre-generate keys for churn phase (generous buffer)
        churn_size = TOTAL_OPERATIONS  # Worst case: all inserts
        print(f"Generating {churn_size:,} keys for churn operations...")
        churn_keys = generate_keys(churn_size, seed + CHURN_SEED_OFFSET)
        print(f"Keys generated.\n")
        
        # Run each variant
        for variant_name, variant_class in VARIANTS:
            results = run_experiment_for_variant(
                variant_name, variant_class, prefill_keys, churn_keys, seed
            )
            all_results.extend(results)
    
    # Write results to CSV
    print(f"\nWriting results to {output_file}...")
    fieldnames = [
        'experiment', 'variant', 'seed', 'batch', 'operations',
        'load_factor', 'size',
        'avg_lookup_ns', 'avg_insert_ns', 'avg_delete_ns',
        'tombstones', 'avg_probe', 'max_probe',
        'avg_chain', 'max_chain',
        'failed_inserts'
    ]
    
    with open(output_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_results)
    
    print(f"Results written. Total rows: {len(all_results)}")
    print("=" * 60)


if __name__ == '__main__':
    run_experiment_2()
