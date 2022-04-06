Daniel Jang, Philip Wang, Suei-Wen Chen

# Logistics

## Design Choices

### Filter and Join

### Aggregates
#### IntegerAggregator

#### StringAggregator
The implementation of `StringAggregator` is analogous to that of `IntegerAggregator`.

#### Aggregate
The `Aggregate` class contains attributes `child` (an `OpIterator` which fetechs tuples to compute aggregation), `afield` (aggregate field index), `gfield` (group-by field index, -1 if no grouping), `aop` (an `Aggregator.Op` specifying which aggregation to perform), `typeAggregator` (a `Aggregator` which is an `IntegerAggregator` if aggregating on integers or a `StringAggregator` if aggregating on string), and `aggIterator` (a `TupleIterator` that can iterate through the resulting set after aggregation).

In `Aggregate.open()`, the aggregate is computed by feeding the tuple we get from the `child` iterator to the `typeAggregator.mergeTupleIntoGroup()` method, and `aggIterator` is initialized as `typeAggregator.iterator()`.

### HeapFile Mutability
At page level, `HeapPage.insertTuple()` inserts a given tuple into a page after checking that there is empty slot and that the schema is compatible, and `HeapPage.deleteTuple()` deletes a given tuple by clearing the bit in the header of the page after checking that the tuple is on the page.

At file level, `HeapFile.insertTuple()` insert a given tuple into a file by finding a page in the file which has an empty slot for insertion and then calls `HeapPage.insertTuple()` on that page, and `HeapFile.deleteTuple()` deletes a given tuple in a file by locating the page on which the tuple is stored using its record id and calls `HeapPage.deleteTuple()` on that page. Both `HeapFile.insertTuple()` and `HeapFile.deleteTuple()` return an ArrayList of pages that are modified.

In the buffer pool, given a tuple and a table id, `BufferPool.insertTuple()` locates the `HeapFile` associated with the table using `getCatalog().getDatabaseFile()` and then calls `HeapPage.insertTuple()` on that page to insert the given tuple. The list of pages returned by `HeapPage.insertTuple()` are marked dirty; the pages that are not originally in the buffer pool are then added using the `BufferPool.getPage()` method, and those origianally in the buffer pool are replaced by the modified version of the pages. `BufferPool.deleteTuple()` works similarly by invoking `HeapFile.deleteTuple()`.


### Insertion and Deletion

For insertion/deletion, we iterated the dirtied pages returned from `heapfile.insertTuple()/heapfile.deleteTuple()`, marked each page as dirty, and added it to the buffer pool heap map. If the dirty page was not in the buffer pool, we called the `getPage` method to check whether we had to evict an existing page.

### Eviction Policy

For our eviction policy, we chose the LRU, in which the page that was inserted/updated in the buffer the earliest gets evicted. In order to implement this, we used the `LinkedHashMap` data structure to store the pages in the buffer pool, and set the `accessOrder` parameter in its constructor. This allowed us to retrieve and evict pages in the buffer pool in the order of access (least recently accessed first). When iterating the buffer pool, because LinkedHashMap treats `get()` as an access to the element, there were cases where we had to create a read-only copy of the LinkedHashMap using `entrySet()`in order to avoid _ConcurrentModificationException_. This would increase the memory usage of the application, but it was the tradeoff we made to ensure the LRU policy.

## Confusing Points

## Time Spent

We spent roughly three full days as a group of three to finish this lab. A significant portion of our time was spent fixing the errors from the system tests, which we found to be much harder to debug than the individual tests.
