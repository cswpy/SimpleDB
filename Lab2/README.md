Daniel Jang, Philip Wang, Steven Chen

# Logistics

## Design Choices

### Filter and Join

### Aggregates

### HeapFile Mutability

### Insertion and Deletion

For insertion/deletion, we iterated the dirtied pages returned from `heapfile.insertTuple()/heapfile.deleteTuple()`, marked each page as dirty, and added it to the buffer pool heap map. If the dirty page was not in the buffer pool, we called the `getPage` method to check whether we had to evict an existing page.

### Eviction Policy

For our eviction policy, we chose the LRU, in which the page that was inserted/updated in the buffer the earliest gets evicted. In order to implement this, we used the `LinkedHashMap` data structure to store the pages in the buffer pool, and set the `accessOrder` parameter in its constructor. This allowed us to retrieve and evict pages in the buffer pool in the order of access (least recently accessed first). When iterating the buffer pool, because LinkedHashMap treats `get()` as an access to the element, there were cases where we had to create a read-only copy of the LinkedHashMap using `entrySet()`in order to avoid _ConcurrentModificationException_. This would increase the memory usage of the application, but it was the tradeoff we made to ensure the LRU policy.

## Confusing Points

## Time Spent

We spent roughly three full days as a group of three to finish this lab. A significant portion of our time was spent fixing the errors from the system tests, which we found to be much more harder to debug than the individual tests.
