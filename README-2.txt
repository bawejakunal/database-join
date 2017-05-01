==========================================
UNI: kb2896
NAME: Kunal Baweja
COMS W4112 Database Systems Implementation
Project - 2, Part - 2 (with Bonus Part)

Detailed comments in q4112.c
==========================================

Experimental Setup:
    Clic Machine: beirut
    RAM: 24 GB
    Max Threads: 16
    Threads for each config: 1, 2, 4, 8, 16

Files attached:
    q4112.c - source code for multithreaded in-memory hash join
    q4112.csv - 525 lines of csv for experiments run on each of 21 given
    configurations


Observations:
    1.  For each of the configurations I observed that doubling the number
        of threads approximately halves the query run time.
        But increasing number of threads from 8 to 16 gives less than double
        performance improvement.

    2. In part 2, calculating aggregate over the inner hash table incurs more
        cost. If multiple threads try to atomically update the aggregates in
        global aggregation table over a small number of aggregation keys it
        may cause contention, which can be overcome with the help of thread
        local hash tables, implemented like a direct mapping cache. Whenever
        there is a collision in inserting or updating a record in the thread
        local cache, the cache item is flushed to global aggregate table and
        then overwritten with the incoming item.

    3. For configurations with 50% or 100% `hard hitter` groups the query
        implementation effectively avoids contention among threads with the
        help of L2 resident thread local caches.

Approach/Algorithm:
    1. The inner table in the join query is built into an in-memory hash
        table, with linear probing (better cache locality). It uses a 
        multiplicative hashing algorithm with Knuth factor as the prime.

    2. For building the in-memory hash table, I divide up the given inner
        relation of join query into equal sized chunks of records and 
        based on the number of threads requested and each thread builds
        up a separate portion of the hash table for joining tuples of given
        relation, in parallel.

    3. Flajolet-Martin algorithm is used for computing bitvectors in each
        thread, by scanning over a separate portion of the aggregation
        keys (outer relation) table and updating it's own set of bitvectors.
        After this threads are synchronized on a barrier and the last thread
        to reach the barrier, merges the bitvectors computed by each of the
        threads, makes a final estimate for the number of unique aggregation
        keys and allocates memory to aggregation table.

    4. For estimating a the number of unique aggregation keys, I keep the
        estimate as a perfect power of 2, to facilitate better hash indexing
        into the aggregate table. In this strategy, whatever is the estimate
        by calculated by the parallelized Flajolet-Martin estimation, I update
        it to the next value, greater than or equal to the estimate, which is
        a perfect power of 2.
        By this strategy we overestimate a little (1.5X-2X), but that helps in
        the long run by providing hash indexing with less collisions into the
        aggregation table, without affecting performance.

    5. Next each of the thread starts scanning records from outer table,
        joining with the inner hash table constructed previously to calculate
        the value of each order and updates the partial aggregate in the thread
        local cache (direct mapping hash table). In case of collision while
        updating the thread local cache, the cache item is flushed out to the
        global aggregate table. This is the direct-mapping strategy.

    6. Finally, when each thread has finished computing aggregates over their
        respective portion of the outer relation table, they flush out the 
        entries from their local cache tables to the global aggregate table
        and syncrhonize on a fourth barrier.

    7. After synchronizing, the global aggregate table entries are evenly
        allocated to participating threads for computing sum of averages for
        each aggregation key and count of keys scanned by each thread. This
        is returned in the thread info struct as the partial result, which is 
        merged into the final result by the thread that calls join on the
        threads.
