"""
Experiment 1: Load Factor vs Performance

Measures how hashmap performance degrades as load factor increases.
Tests each variant at fixed load factors: 0.25, 0.5, 0.75, 0.85, 0.9, 0.95
"""

import random
import time
import csv
import math
from typing import List, Tuple, Any
from ..hashmaps import CAPACITY, LinearProbingMap, RobinHoodMap, ChainingMap, CuckooMap


def compute_statistics(times_ns: List[float]) -> Tuple[float, float, float, float]:
    """
    Compute statistical measures for a list of timing measurements.
    
    Args:
        times_ns: List of individual timing measurements in nanoseconds
        
    Returns:
        Tuple of (mean, variance, std_dev, coefficient_of_variation)
    """
    if not times_ns:
        return 0.0, 0.0, 0.0, 0.0
    
    n = len(times_ns)
    mean = sum(times_ns) / n
    
    if n < 2:
        return mean, 0.0, 0.0, 0.0
    
    # Variance (sample variance with n-1 denominator)
    variance = sum((t - mean) ** 2 for t in times_ns) / (n - 1)
    
    # Standard deviation
    std_dev = math.sqrt(variance)
    
    # Coefficient of variation (relative standard deviation)
    cv = (std_dev / mean) if mean != 0 else 0.0
    
    return mean, variance, std_dev, cv

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


def measure_insert_phase(hashmap: Any, keys: List[int], target_lf: float) -> Tuple[float, float, float, float, int, bool]:
    """
    Insert keys until target load factor is reached, measuring latency statistics.
    
    Args:
        hashmap: The hashmap instance
        keys: List of keys to insert
        target_lf: Target load factor
        
    Returns:
        Tuple of (avg_ns, variance_ns, std_dev_ns, cv, keys_inserted, success)
    """
    target_size = int(CAPACITY * target_lf)
    current_size = hashmap.size
    keys_to_insert = target_size - current_size
    
    if keys_to_insert <= 0:
        return 0.0, 0.0, 0.0, 0.0, 0, True
    
    insert_times_ns = []
    
    for i in range(keys_to_insert):
        key = keys[current_size + i]
        
        start = time.perf_counter()
        result = hashmap.insert(key, i)
        elapsed = time.perf_counter() - start
        
        if not result.success:
            # Insertion failed (likely Cuckoo)
            if insert_times_ns:
                mean, variance, std_dev, cv = compute_statistics(insert_times_ns)
                return mean, variance, std_dev, cv, len(insert_times_ns), False
            return 0.0, 0.0, 0.0, 0.0, 0, False
        
        insert_times_ns.append(elapsed * 1e9)  # Convert to nanoseconds
    
    mean, variance, std_dev, cv = compute_statistics(insert_times_ns)
    return mean, variance, std_dev, cv, len(insert_times_ns), True


def measure_lookup_phase(hashmap: Any, keys: List[int], sample_size: int, seed: int) -> Tuple[float, float, float, float]:
    """
    Perform random lookups and measure latency statistics.
    
    Args:
        hashmap: The hashmap instance
        keys: List of keys that exist in the hashmap
        sample_size: Number of lookups to perform
        seed: Random seed for selecting keys
        
    Returns:
        Tuple of (avg_ns, variance_ns, std_dev_ns, coefficient_of_variation)
    """
    rng = random.Random(seed)
    
    # Sample random keys from those inserted
    lookup_keys = rng.choices(keys[:hashmap.size], k=sample_size)
    
    lookup_times_ns = []
    
    for key in lookup_keys:
        start = time.perf_counter()
        result, value = hashmap.lookup(key)
        elapsed = time.perf_counter() - start
        lookup_times_ns.append(elapsed * 1e9)  # Convert to nanoseconds
    
    return compute_statistics(lookup_times_ns)


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
        insert_avg_ns, insert_var_ns, insert_std_ns, insert_cv, inserted, success = measure_insert_phase(hashmap, keys, target_lf)
        
        if not success:
            print(f"    {variant_name} failed at LF={hashmap.load_factor():.3f} (target was {target_lf})")
            break
        
        # Lookup phase
        lookup_avg_ns, lookup_var_ns, lookup_std_ns, lookup_cv = measure_lookup_phase(hashmap, keys, LOOKUP_SAMPLE_SIZE, seed + LOOKUP_SEED_OFFSET)
        
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
            'avg_insert_ns': insert_avg_ns,
            'var_insert_ns': insert_var_ns,
            'std_insert_ns': insert_std_ns,
            'cv_insert': insert_cv,
            'avg_lookup_ns': lookup_avg_ns,
            'var_lookup_ns': lookup_var_ns,
            'std_lookup_ns': lookup_std_ns,
            'cv_lookup': lookup_cv,
            'tombstones': metrics.get('tombstones', 0),
            'avg_probe': metrics.get('avg_probe', 0.0),
            'max_probe': metrics.get('max_probe', 0),
            'avg_chain': metrics.get('avg_chain', 0.0),
            'max_chain': metrics.get('max_chain', 0),
            'failed_inserts': metrics.get('failed_inserts', 0),
        }
        results.append(result)
        
        print(f"    LF={target_lf}: insert={insert_avg_ns:.1f}ns (σ={insert_std_ns:.1f}), lookup={lookup_avg_ns:.1f}ns (σ={lookup_std_ns:.1f})")
    
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
        'avg_insert_ns', 'var_insert_ns', 'std_insert_ns', 'cv_insert',
        'avg_lookup_ns', 'var_lookup_ns', 'std_lookup_ns', 'cv_lookup',
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
