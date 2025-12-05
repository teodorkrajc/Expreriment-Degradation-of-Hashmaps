"""
Common utilities for hashmap experiments.
Implements the uniform 64-bit hash mixing function and shared constants.
"""

# Fixed capacity for all experiments
CAPACITY = 1 << 20  # 2^20 = 1,048,576


def mix(x: int) -> int:
    """
    Uniform 64-bit hash mixing function.
    
    Args:
        x: 64-bit integer key
        
    Returns:
        Mixed 64-bit hash value
    """
    # Ensure we're working with 64-bit unsigned integers
    x = x & 0xFFFFFFFFFFFFFFFF
    
    x ^= x >> 30
    x = (x * 0xbf58476d1ce4e5b9) & 0xFFFFFFFFFFFFFFFF
    x ^= x >> 27
    x = (x * 0x94d049bb133111eb) & 0xFFFFFFFFFFFFFFFF
    x ^= x >> 31
    
    return x & 0xFFFFFFFFFFFFFFFF


def get_index(key: int) -> int:
    """
    Get index for single-hash variants (Linear Probing, Robin Hood, Chaining).
    
    Args:
        key: 64-bit integer key
        
    Returns:
        Index in range [0, CAPACITY)
    """
    return mix(key) & (CAPACITY - 1)


def get_cuckoo_indices(key: int) -> tuple[int, int]:
    """
    Get two indices for Cuckoo hashing.
    
    Args:
        key: 64-bit integer key
        
    Returns:
        Tuple of (index1, index2)
    """
    h = mix(key)
    i1 = h & (CAPACITY - 1)
    i2 = (h >> 32) & (CAPACITY - 1)
    return i1, i2


class OpResult:
    """Result of a hashmap operation with metrics."""
    
    def __init__(self, success: bool = False, probe_count: int = 0, displacement: int = 0):
        self.success = success
        self.probe_count = probe_count
        self.displacement = displacement
