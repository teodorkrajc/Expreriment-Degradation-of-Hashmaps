"""
Linear Probing Hashmap Implementation.

Open addressing with tombstones for deletions.
Tracks probe counts and tombstone accumulation.
"""

from enum import Enum
from typing import Optional
from common import CAPACITY, get_index, OpResult


class State(Enum):
    """Entry state in the hash table."""
    EMPTY = 0
    OCCUPIED = 1
    TOMBSTONE = 2


class Entry:
    """Hash table entry."""
    
    def __init__(self):
        self.key: Optional[int] = None
        self.value: Optional[int] = None
        self.state: State = State.EMPTY


class LinearProbingMap:
    """
    Linear Probing hashmap with fixed capacity.
    
    Uses tombstones for deletion to maintain probe sequences.
    """
    
    def __init__(self):
        self.capacity = CAPACITY
        self.size = 0  # Number of occupied entries
        self.tombstones = 0  # Number of tombstone entries
        self.table = [Entry() for _ in range(CAPACITY)]
    
    def insert(self, key: int, value: int) -> OpResult:
        """
        Insert or update a key-value pair.
        
        Args:
            key: 64-bit integer key
            value: Integer value
            
        Returns:
            OpResult with success status and probe count
        """
        result = OpResult()
        index = get_index(key)
        original_index = index
        
        while True:
            result.probe_count += 1
            entry = self.table[index]
            
            if entry.state == State.EMPTY or entry.state == State.TOMBSTONE:
                # Found empty or tombstone slot
                if entry.state == State.TOMBSTONE:
                    self.tombstones -= 1
                entry.key = key
                entry.value = value
                entry.state = State.OCCUPIED
                self.size += 1
                result.success = True
                return result
            elif entry.state == State.OCCUPIED and entry.key == key:
                # Key already exists, update value
                entry.value = value
                result.success = True
                return result
            
            # Linear probing: move to next slot
            index = (index + 1) & (self.capacity - 1)
            
            if index == original_index:
                # Table is full
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
        original_index = index
        
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
            
            # Continue probing (skip tombstones)
            index = (index + 1) & (self.capacity - 1)
            
            if index == original_index:
                # Full cycle without finding key
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
        original_index = index
        
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
            
            # Continue probing
            index = (index + 1) & (self.capacity - 1)
            
            if index == original_index:
                # Key not found
                return result
    
    def load_factor(self) -> float:
        """Get current load factor."""
        return self.size / self.capacity
    
    def tombstone_count(self) -> int:
        """Get number of tombstones."""
        return self.tombstones
