"""
Chaining Hashmap Implementation.

Array of buckets with linked lists for collision resolution.
Tracks average and maximum chain lengths.
"""

from typing import Optional
from .common import CAPACITY, get_index, OpResult


class Node:
    """Linked list node for chaining."""
    
    def __init__(self, key: int, value: int):
        self.key = key
        self.value = value
        self.next: Optional[Node] = None


class ChainingMap:
    """
    Chaining hashmap with fixed capacity.
    
    Uses separate chaining with linked lists for collision resolution.
    """
    
    def __init__(self):
        self.capacity = CAPACITY
        self.size = 0  # Total number of key-value pairs
        self.buckets: list[Optional[Node]] = [None] * CAPACITY
    
    def insert(self, key: int, value: int) -> OpResult:
        """
        Insert or update a key-value pair.
        
        Args:
            key: 64-bit integer key
            value: Integer value
            
        Returns:
            OpResult with success status and chain traversal count
        """
        result = OpResult()
        index = get_index(key)
        
        # Check if key already exists
        current = self.buckets[index]
        while current is not None:
            result.probe_count += 1
            if current.key == key:
                # Update existing key
                current.value = value
                result.success = True
                return result
            current = current.next
        
        # Key not found, insert at head of chain
        new_node = Node(key, value)
        new_node.next = self.buckets[index]
        self.buckets[index] = new_node
        self.size += 1
        result.probe_count += 1
        result.success = True
        return result
    
    def lookup(self, key: int) -> tuple[OpResult, Optional[int]]:
        """
        Lookup a key in the hashmap.
        
        Args:
            key: 64-bit integer key
            
        Returns:
            Tuple of (OpResult with chain traversal count, value if found else None)
        """
        result = OpResult()
        index = get_index(key)
        
        current = self.buckets[index]
        while current is not None:
            result.probe_count += 1
            if current.key == key:
                result.success = True
                return result, current.value
            current = current.next
        
        # Key not found
        return result, None
    
    def delete(self, key: int) -> OpResult:
        """
        Delete a key from the hashmap.
        
        Args:
            key: 64-bit integer key
            
        Returns:
            OpResult with success status and chain traversal count
        """
        result = OpResult()
        index = get_index(key)
        
        current = self.buckets[index]
        prev = None
        
        while current is not None:
            result.probe_count += 1
            if current.key == key:
                # Found the key, remove it
                if prev is None:
                    # Remove head of chain
                    self.buckets[index] = current.next
                else:
                    # Remove from middle or end
                    prev.next = current.next
                self.size -= 1
                result.success = True
                return result
            prev = current
            current = current.next
        
        # Key not found
        return result
    
    def load_factor(self) -> float:
        """Get current load factor."""
        return self.size / self.capacity
    
    def get_chain_length(self, index: int) -> int:
        """Get length of chain at given index."""
        length = 0
        current = self.buckets[index]
        while current is not None:
            length += 1
            current = current.next
        return length
    
    def get_avg_chain_length(self) -> float:
        """Get average chain length across all non-empty buckets."""
        non_empty_buckets = 0
        total_length = 0
        
        for i in range(self.capacity):
            if self.buckets[i] is not None:
                non_empty_buckets += 1
                total_length += self.get_chain_length(i)
        
        if non_empty_buckets == 0:
            return 0.0
        return total_length / non_empty_buckets
    
    def get_max_chain_length(self) -> int:
        """Get maximum chain length in the table."""
        max_length = 0
        for i in range(self.capacity):
            if self.buckets[i] is not None:
                length = self.get_chain_length(i)
                max_length = max(max_length, length)
        return max_length
