"""
Experiment 1: Load Factor vs Performance

Measures how hashmap performance degrades as load factor increases.
Tests each variant at fixed load factors: 0.25, 0.5, 0.75, 0.85, 0.9, 0.95
"""

import random
import time
import csv
from typing import List, Tuple, Any
from hashmaps import CAPACITY, LinearProbingMap, RobinHoodMap, ChainingMap, CuckooMap

# Hashmap variants to test
VARIANTS = [
    ('LP', LinearProbingMap),
    ('RH', RobinHoodMap),
    ('CH', ChainingMap),
    ('CU', CuckooMap),
]

# Load factors to test
LOAD_FACTORS = [0.25, 0.5, 0.75, 0.85, 0.9, 0.95]

# Number of lookups to perform at each load factor (average is taken over these lookups)
LOOKUP_SAMPLE_SIZE = 10000

# Seeds for reproducibility (can be extended to multiple runs)
SEEDS = [42]  # Add more seeds like [42, 123, 456] for multiple runs

# Seed offset for lookup phase to avoid correlation with key generation
LOOKUP_SEED_OFFSET = 1000


def generate_keys(count: int, seed: int) -> List[int]:
    """
    Generate random 64-bit integer keys.
    
    Args:
        count: Number of keys to generate
        seed: Random seed for reproducibility
        
    Returns:
        List of unique random keys
    """
    rng = random.Random(seed)
    keys = set()
    while len(keys) < count:
        # Generate random 64-bit integers
        key = rng.randint(0, 2**64 - 1)
        keys.add(key)
    return list(keys)


def measure_insert_phase(hashmap: Any, keys: List[int], target_lf: float) -> Tuple[float, int, bool]:
    """
    Insert keys until target load factor is reached, measuring average latency.
    
    Args:
        hashmap: The hashmap instance
        keys: List of keys to insert
        target_lf: Target load factor
        
    Returns:
        Tuple of (avg_insert_latency_ns, keys_inserted, success)
    """
    target_size = int(CAPACITY * target_lf)
    current_size = hashmap.size
    keys_to_insert = target_size - current_size
    
    if keys_to_insert <= 0:
        return 0.0, 0, True
    
    inserted = 0
    
    # Time the entire insertion phase at once to minimize timer overhead
    # (starting/stopping timer millions of times adds significant measurement noise)
    start = time.perf_counter()
    
    for i in range(keys_to_insert):
        key = keys[current_size + i]
        result = hashmap.insert(key, i)
        
        if not result.success:
            # Insertion failed (likely Cuckoo)
            elapsed = time.perf_counter() - start
            return (elapsed / inserted * 1e9) if inserted > 0 else 0.0, inserted, False
        
        inserted += 1
    
    elapsed = time.perf_counter() - start
    
    # Convert to nanoseconds
    avg_latency_ns = (elapsed / inserted) * 1e9
    return avg_latency_ns, inserted, True


def measure_lookup_phase(hashmap: Any, keys: List[int], sample_size: int, seed: int) -> float:
    """
    Perform random lookups and measure average latency.
    
    Args:
        hashmap: The hashmap instance
        keys: List of keys that exist in the hashmap
        sample_size: Number of lookups to perform
        seed: Random seed for selecting keys
        
    Returns:
        Average lookup latency in nanoseconds
    """
    rng = random.Random(seed)
    
    # Sample random keys from those inserted
    lookup_keys = rng.choices(keys[:hashmap.size], k=sample_size)
    
    # Time the entire lookup phase at once to minimize timer overhead
    start = time.perf_counter()
    
    for key in lookup_keys:
        result, value = hashmap.lookup(key)
    
    elapsed = time.perf_counter() - start
    
    # Convert to nanoseconds
    return (elapsed / sample_size) * 1e9


# TODO: potentially unnecessary
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
        # Linear Probing and Robin Hood
        metrics['tombstones'] = hashmap.tombstone_count()
        
        if variant == 'RH':
            metrics['avg_probe'] = hashmap.get_avg_probe_length()
            metrics['max_probe'] = hashmap.get_max_probe_length()
    
    elif variant == 'CH':
        # Chaining
        metrics['avg_chain'] = hashmap.get_avg_chain_length()
        metrics['max_chain'] = hashmap.get_max_chain_length()
    
    elif variant == 'CU':
        # Cuckoo
        metrics['failed_inserts'] = hashmap.get_failed_inserts()
    
    return metrics


def run_experiment_for_variant(variant_name: str, variant_class: Any, keys: List[int], seed: int) -> List[dict]:
    """
    Run experiment 1 for a single hashmap variant.
    
    Args:
        variant_name: Name of variant (LP, RH, CH, CU)
        variant_class: Class of the hashmap
        keys: Pre-generated keys
        seed: Seed for this run
        
    Returns:
        List of result dictionaries
    """
    print(f"  Running {variant_name}...")
    
    hashmap = variant_class()
    results = []
    
    for target_lf in LOAD_FACTORS:
        # Insert phase
        avg_insert_ns, inserted, success = measure_insert_phase(hashmap, keys, target_lf)
        
        if not success:
            print(f"    {variant_name} failed at LF={hashmap.load_factor():.3f} (target was {target_lf})")
            break
        
        # Lookup phase
        avg_lookup_ns = measure_lookup_phase(hashmap, keys, LOOKUP_SAMPLE_SIZE, seed + LOOKUP_SEED_OFFSET)
        
        # Collect metrics
        metrics = collect_metrics(hashmap, variant_name)
        
        # Record results
        result = {
            'experiment': 'exp1',
            'variant': variant_name,
            'seed': seed,
            'target_lf': target_lf,
            'actual_lf': metrics['load_factor'],
            'size': metrics['size'],
            'avg_insert_ns': avg_insert_ns,
            'avg_lookup_ns': avg_lookup_ns,
            'tombstones': metrics.get('tombstones', 0),
            'avg_probe': metrics.get('avg_probe', 0.0),
            'max_probe': metrics.get('max_probe', 0),
            'avg_chain': metrics.get('avg_chain', 0.0),
            'max_chain': metrics.get('max_chain', 0),
            'failed_inserts': metrics.get('failed_inserts', 0),
        }
        results.append(result)
        
        print(f"    LF={target_lf}: insert={avg_insert_ns:.1f}ns, lookup={avg_lookup_ns:.1f}ns")
    
    return results


def run_experiment_1(output_file: str = 'results/experiment_1_results.csv'):
    """
    Run Experiment 1 for all variants and all seeds.
    
    Args:
        output_file: Path to output CSV file (relative to project root)
    """
    print("=" * 60)
    print("EXPERIMENT 1: Load Factor vs Performance")
    print("=" * 60)
    
    all_results = []
    
    # run for each seed
    for seed in SEEDS:
        print(f"\nRun with seed={seed}")
        print("-" * 60)
        
        # Pre-generate keys for maximum possible size (LF=0.95)
        max_keys_needed = int(CAPACITY * 0.95) + 1000  # Extra for safety
        print(f"Generating {max_keys_needed:,} unique keys...")
        keys = generate_keys(max_keys_needed, seed)
        print(f"Keys generated.\n")
        
        # Run each variant
        for variant_name, variant_class in VARIANTS:
            results = run_experiment_for_variant(variant_name, variant_class, keys, seed)
            all_results.extend(results)
    
    # Write results to CSV
    print(f"\nWriting results to {output_file}...")
    fieldnames = [
        'experiment', 'variant', 'seed', 'target_lf', 'actual_lf', 'size',
        'avg_insert_ns', 'avg_lookup_ns',
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
    run_experiment_1()
