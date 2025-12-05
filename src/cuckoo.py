"""
Cuckoo Hashing Implementation.

Two-choice hashing with displacement and relocation on conflict.
Tracks displacement counts and failed insertions.
"""

from typing import Optional
from common import CAPACITY, get_cuckoo_indices, OpResult


# Maximum number of displacement attempts before considering insertion failed
MAX_DISPLACEMENTS = 500


class Entry:
    """Hash table entry."""
    
    def __init__(self):
        self.key: Optional[int] = None
        self.value: Optional[int] = None
        self.occupied: bool = False


class CuckooMap:
    """
    Cuckoo Hashing with fixed capacity.
    
    Uses two hash functions for two-choice hashing.
    On collision, kicks out existing key and relocates it.
    """
    
    def __init__(self):
        self.capacity = CAPACITY
        self.size = 0
        self.failed_inserts = 0  # Count of failed insertions
        self.table = [Entry() for _ in range(CAPACITY)]
    
    def insert(self, key: int, value: int) -> OpResult:
        """
        Insert or update a key-value pair using cuckoo strategy.
        
        Args:
            key: 64-bit integer key
            value: Integer value
            
        Returns:
            OpResult with success status and displacement count
        """
        result = OpResult()
        i1, i2 = get_cuckoo_indices(key)
        
        # Check if key already exists at either position
        if self.table[i1].occupied and self.table[i1].key == key:
            self.table[i1].value = value
            result.success = True
            return result
        if self.table[i2].occupied and self.table[i2].key == key:
            self.table[i2].value = value
            result.success = True
            return result
        
        # Try to insert at first position
        if not self.table[i1].occupied:
            self.table[i1].key = key
            self.table[i1].value = value
            self.table[i1].occupied = True
            self.size += 1
            result.success = True
            return result
        
        # Try to insert at second position
        if not self.table[i2].occupied:
            self.table[i2].key = key
            self.table[i2].value = value
            self.table[i2].occupied = True
            self.size += 1
            result.success = True
            return result
        
        # Both positions occupied, start displacement cycle
        current_key = key
        current_value = value
        current_index = i1
        
        for displacement in range(MAX_DISPLACEMENTS):
            result.displacement += 1
            
            # Kick out the element at current_index
            evicted_key = self.table[current_index].key
            evicted_value = self.table[current_index].value
            
            # Place current key-value at current_index
            self.table[current_index].key = current_key
            self.table[current_index].value = current_value
            
            # Check if this was a new insertion
            if displacement == 0:
                self.size += 1
            
            # Try to relocate the evicted element
            i1, i2 = get_cuckoo_indices(evicted_key)
            
            # Choose the alternative position (not current_index)
            if i1 == current_index:
                next_index = i2
            else:
                next_index = i1
            
            # If next position is empty, we're done
            if not self.table[next_index].occupied:
                self.table[next_index].key = evicted_key
                self.table[next_index].value = evicted_value
                self.table[next_index].occupied = True
                result.success = True
                return result
            
            # Continue displacement cycle
            current_key = evicted_key
            current_value = evicted_value
            current_index = next_index
        
        # Failed to insert after MAX_DISPLACEMENTS attempts
        self.failed_inserts += 1
        result.success = False
        return result
    
    def lookup(self, key: int) -> tuple[OpResult, Optional[int]]:
        """
        Lookup a key in the hashmap.
        
        Args:
            key: 64-bit integer key
            
        Returns:
            Tuple of (OpResult, value if found else None)
        """
        result = OpResult()
        i1, i2 = get_cuckoo_indices(key)
        
        # Check first position
        result.probe_count += 1
        if self.table[i1].occupied and self.table[i1].key == key:
            result.success = True
            return result, self.table[i1].value
        
        # Check second position
        result.probe_count += 1
        if self.table[i2].occupied and self.table[i2].key == key:
            result.success = True
            return result, self.table[i2].value
        
        # Key not found
        return result, None
    
    def delete(self, key: int) -> OpResult:
        """
        Delete a key from the hashmap.
        
        Args:
            key: 64-bit integer key
            
        Returns:
            OpResult with success status
        """
        result = OpResult()
        i1, i2 = get_cuckoo_indices(key)
        
        # Check first position
        result.probe_count += 1
        if self.table[i1].occupied and self.table[i1].key == key:
            self.table[i1].occupied = False
            self.size -= 1
            result.success = True
            return result
        
        # Check second position
        result.probe_count += 1
        if self.table[i2].occupied and self.table[i2].key == key:
            self.table[i2].occupied = False
            self.size -= 1
            result.success = True
            return result
        
        # Key not found
        return result
    
    def load_factor(self) -> float:
        """Get current load factor."""
        return self.size / self.capacity
    
    def get_failed_inserts(self) -> int:
        """Get count of failed insertions."""
        return self.failed_inserts
