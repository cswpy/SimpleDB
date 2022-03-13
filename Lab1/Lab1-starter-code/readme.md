Daniel Jang, Philip Wang, Steven Chen

# Logistics

## Design Choices

### Catalog

The catalog contains an array list of tables `ArrayList<Table> tableList`, where a table contains attributes `DbFile file`, `int tableId`, `String tableName`, `TupleDesc Schema`, and `String pKey` (primary key). The `tableId` of a table is the same as the `table_id` of the attribute `file` in order to uniquely associate a table and a heapfile.

### TupleDesc

We used `ArrayList<TDItem>` as a container for storing the `TDItem` lists defined in a TupleDesc. ArrayList provides constant time for insertions and has already implemented an iterator over its elements.

### Tuple

Tuple stores its collection of `Field` objects using `fields`, a fixed array of size given by its `TupleDesc`. Because the size of `fields` is not dynamic, we did not use an ArrayList in our implementation of `*iterator`, but instead overrode the method `hasNext()` and `next()`.

### HeapPage

Every instance of a HeapPage would use pageSize from the `BufferPool` instance and tuple size retrieved from its `TupleDesc` to calculate its maximum number of tuples and header size in bytes. To check whether the ith slot is occupied, we first calculated `header_byte` to determine which header byte to examine, and then `header_bit` to determine which of the 8 bits in the header_byte to check. A simple bit operation would determine whether the `header_bit`th bit at header_byte is 0 or 1. To get the total number of empty slots, we iterated over all possible slots of the HeapPage and counted the empty ones. In our implementation of the iterator, we first constructed an array list, iterated over all slots and added non-empty ones to the array list.

### HeapFile

Upon the creation of a table, HeapFile will generate a unique table id from the absolute path of the data file. To read a particular page in a HeapFile, it uses the `RandomAccessFile` to seek the correct position and read exactly `PAGE_SIZE` number of bytes. To obtain all the tuples in a HeapFile, we first fetch the `BufferPool` instance from the database and query the first-ordered page in the HeapFile from the BufferPool. Once the tuples over a page are exausted, we first check whether there is a next page. If positive, we then get an iterator over all the tuples on the next page and store it inside the `HeapFileIterator` instance. If not, it means we have exausted all the tuples of all the pages in this HeapFile.

### SeqScan

The class `SeqScan` implements `OpIterator` and contains attributes `TransactionId tid`, `int tableid`, `String tableAlias`, and `DbFileIterator iterator`. The `tableAlias` helps support aliasing tables in sql commands, and the `iterator` supports the functionality of traversing data records in the associated `HeapFile`.

## Confusing Points

The relationship between catalog, bufferpool, HeapFile, and HeapPages are unclear at first.

## Time Spent

We spent roughly two full days as a group of three to finish this lab. A significant portion of our time was spent familiarizing ourselves with Java OOP.
