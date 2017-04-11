==========================================
UNI: kb2896
NAME: Kunal Baweja
COMS W4112 Database Systems Implementation
Project - 2, Part - 1
==========================================

Experimental Setup:
    Clic Machine: baghdad
    RAM: 24 GB
    Max Threads: 16
    Threads for each config: 1, 2, 4, 8, 16

Files attached:
    q4112_hj.c - source code for multithreaded in-memory hash join
    q4112_hj.csv - 200 lines of csv for experiments run on each of
    8 given configurations


Observations:
    1.  For each of the configurations I observed that doubling the number
        of threads approximately halves the query run time.

        But increasing number of threads from 8 to 16 gives less than double
        performance improvement.

    2.  In pairs of configuration (1, 2) and (3, 4) increasing the selectivity
        of outer relation from 0.5 to 1, leads to approximate doubling in time 
        of query, for equal number of threads. 

        This is because of very small size of hash table in comparison to 
        outer table, which largely determines the runtime of query.

    3. In pairs of configurations (5, 6) and (7, 8) increasing the selectivity
        of inner relation from 0.5 to 1, slightly increases the run time
        for equal number of threads used.

        The reason is it takes a large amount of time to construct the huge
        in-memory index of inner relation, which dominates the run time of 
        query and thus halving or doubling the inner selectively makes little
        difference in run time.

Approach/Algorithm:
    1. The inner table in the join query is built into an in-memory hash
        table, with linear probing (better cache locality). It uses a 
        multiplicative hashing algorithm with Knuth factor as the prime.

    2. For building the in-memory hash table, I divide up the given inner
        relation of join query into equal sized chunks of records and 
        based on the number of threads requested.

    3. Each thread inserts it's chunk of inner relation records into the
        global hash table (created before spawning threads), hashing the
        primary keys of each record and inserting them into matchin buckets,
        resolving collisions via linear probing.

    4. Once a thread finishes inserting it's chunk of records into hash
        table, it waits on a barrier for other threads to finish their
        insertions and thus synchronize for the next part of query where
        values are read from the hash table by all threads.

    5. After all threads reach the barrier and have synchronized, they
        proceed to perform the reads. For this the outer relation
        is divided up into equal number of chunks of records, assigned
        to each of the threads.

    6. Each thread hashes the primary key of each outer relation record
        from its respective chunk of records, compares against the hash
        key of the in-memory hash table of inner table and takes a product
        of order quantity(outer record) and price(inner product hashed table)
        and adds to the partial result that it is computing.

    7. Each thread returns its partial result for the average query in a
        as sum and count for a partial number of orders, that it computed.

    8. The calling thread (in query_run function) collects partial sum and
        counts computed by each of the threads, aggregates the result
        and returns the total average value of orders.
        It also joins all the threads and destroys the initiated barrier.
