"""
Robin Hood Hashing Implementation.

Open addressing with distance-based position stealing.
Uses tombstones for deletions and tracks probe lengths (DIB - Distance from Initial Bucket).
"""

from typing import Optional
from .common import CAPACITY, get_index, OpResult, State, Entry


class RobinHoodMap:
    """
    Robin Hood Hashing with fixed capacity.
    
    Inserts may steal positions based on DIB (Distance from Initial Bucket).
    Uses tombstones for deletion.
    """
    
    def __init__(self):
        self.capacity = CAPACITY
        self.size = 0
        self.tombstones = 0
        self.table = [Entry() for _ in range(CAPACITY)]
    
    def insert(self, key: int, value: int) -> OpResult:
        """
        Insert or update a key-value pair using Robin Hood strategy.
        
        Args:
            key: 64-bit integer key
            value: Integer value
            
        Returns:
            OpResult with success status and probe count
        """
        result = OpResult()
        index = get_index(key)
        current_key = key
        current_value = value
        current_dib = 0
        original_index = index
        
        while True:
            result.probe_count += 1
            entry = self.table[index]
            
            if entry.state == State.EMPTY or entry.state == State.TOMBSTONE:
                # Found empty or tombstone slot
                if entry.state == State.TOMBSTONE:
                    self.tombstones -= 1
                entry.key = current_key
                entry.value = current_value
                entry.dib = current_dib
                entry.state = State.OCCUPIED
                self.size += 1
                result.success = True
                return result
            elif entry.state == State.OCCUPIED and entry.key == current_key:
                # Key already exists, update value
                entry.value = current_value
                result.success = True
                return result
            elif entry.state == State.OCCUPIED and current_dib > entry.dib:
                # Robin Hood: steal from the rich (higher DIB takes the slot)
                # Swap current with entry
                current_key, entry.key = entry.key, current_key
                current_value, entry.value = entry.value, current_value
                current_dib, entry.dib = entry.dib, current_dib
            
            # Move to next slot
            current_dib += 1
            index = (index + 1) & (self.capacity - 1)
            
            if index == original_index and current_dib >= self.capacity:
                # Prevent infinite loop
                result.success = False
                return result
    
    def lookup(self, key: int) -> tuple[OpResult, Optional[int]]:
        """
        Lookup a key in the hashmap.
        
        Args:
            key: 64-bit integer key
            
        Returns:
            Tuple of (OpResult with probe count, value if found else None)
        """
        result = OpResult()
        index = get_index(key)
        dib = 0
        
        while True:
            result.probe_count += 1
            entry = self.table[index]
            
            if entry.state == State.EMPTY:
                # Key not found
                return result, None
            elif entry.state == State.OCCUPIED and entry.key == key:
                # Key found
                result.success = True
                return result, entry.value
            elif entry.state == State.OCCUPIED and dib > entry.dib:
                # If our DIB is greater than the entry's DIB, the key isn't here
                # (Robin Hood property: we would have stolen this spot)
                return result, None
            
            # Continue probing
            dib += 1
            index = (index + 1) & (self.capacity - 1)
            
            if dib >= self.capacity:
                # Prevent infinite loop
                return result, None
    
    def delete(self, key: int) -> OpResult:
        """
        Delete a key from the hashmap.
        
        Args:
            key: 64-bit integer key
            
        Returns:
            OpResult with success status and probe count
        """
        result = OpResult()
        index = get_index(key)
        dib = 0
        
        while True:
            result.probe_count += 1
            entry = self.table[index]
            
            if entry.state == State.EMPTY:
                # Key not found
                return result
            elif entry.state == State.OCCUPIED and entry.key == key:
                # Key found, mark as tombstone
                entry.state = State.TOMBSTONE
                self.size -= 1
                self.tombstones += 1
                result.success = True
                return result
            elif entry.state == State.OCCUPIED and dib > entry.dib:
                # Key not present (Robin Hood property)
                return result
            
            # Continue probing
            dib += 1
            index = (index + 1) & (self.capacity - 1)
            
            if dib >= self.capacity:
                return result
    
    def load_factor(self) -> float:
        """Get current load factor."""
        return self.size / self.capacity
    
    def tombstone_count(self) -> int:
        """Get number of tombstones."""
        return self.tombstones
    
    def get_max_probe_length(self) -> int:
        """Get maximum DIB (probe length) in the table."""
        max_dib = 0
        for entry in self.table:
            if entry.state == State.OCCUPIED:
                max_dib = max(max_dib, entry.dib)
        return max_dib
    
    def get_avg_probe_length(self) -> float:
        """Get average DIB (probe length) for occupied entries."""
        if self.size == 0:
            return 0.0
        total_dib = sum(entry.dib for entry in self.table if entry.state == State.OCCUPIED)
        return total_dib / self.size
