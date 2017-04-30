/* Copyright 2017 Kunal Baweja
 *
 * UNI: kb2896
 * NAME: Kunal Baweja
 * COMS W4112 Database Systems Implementation
 **/

#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdbool.h>
#include <stdio.h>

// Knuth factor
static const uint64_t HASH_FACTOR = 0x9e3779b1;
static const float  PHI = 0.77351;

// hash bucket struct
typedef struct {
  uint32_t key;
  uint32_t val;
} bucket_t;

// thread info struct
typedef struct {
    pthread_t id;
    int thread;
    int threads;
    size_t inner_tuples;
    size_t outer_tuples;
    const uint32_t* inner_keys;
    const uint32_t* inner_vals;
    const uint32_t* outer_keys;
    const uint32_t* outer_aggr_keys;
    const uint32_t* outer_vals;

    // pass bucket info to threads
    int8_t log_buckets;
    size_t buckets;
    bucket_t* table;
    // thread barrier
    pthread_barrier_t *barrier;
    // Flajolet-Martin estimation values
    uint32_t **bitmaps;
    int8_t log_partitions;
    int8_t *log_estimate;
    // partial results
    uint64_t avg;
    uint32_t count;
} q4112_run_info_t;

// merged result struct
typedef struct {
    uint64_t avg;
    uint32_t count;
} result_t;

// global aggregate table struct
typedef struct
{
    uint32_t key;   // aggregate key
    uint64_t sum;   // aggregate sum
    uint32_t count; // aggregate count
} aggr_t;

// pointer to global aggregate table
// shared across threads
aggr_t *aggr_tbl;


// count trailing zeroes in binary representation
uint32_t count_trailing_zeros(uint32_t value){
    uint32_t count = 32;
    value &= -value;
    if (value) count--;
    if (value & 0x0000FFFF) count -= 16;
    if (value & 0x00FF00FF) count -= 8;
    if (value & 0x0F0F0F0F) count -= 4;
    if (value & 0x33333333) count -= 2;
    if (value & 0x55555555) count -= 1;
    return count;
}

// estimate the number of unique group by keys from outer table
void _estimate(uint32_t *bitmaps, const int8_t log_partitions, 
    const uint32_t* keys, size_t size) {
    size_t h, i, p;
    size_t partitions = 1 << log_partitions;

    for (i = 0; i < size; i++) {
        h = keys[i] * HASH_FACTOR;  // multiplicative hash
        p = h & (partitions - 1);  // use some hash bits to partition
        h >>= log_partitions;  // use remaining hash bits for the bitmap
        bitmaps[p] |= h & -h;  // update bitmap of partition
    }
    return;
}


// fills given hash table
int inner_hash_table(bucket_t* table,
    const size_t begin,
    const size_t end,
    const uint32_t* keys,
    const uint32_t* vals,
    const int8_t log_buckets,
    const size_t buckets) {

    // iterator and hash key variables
    size_t i, h;

    // build table from begin to end-1
    for (i = begin; i < end; ++i) {
        //  multiplicative hashing
        h = (uint32_t) (keys[i] * HASH_FACTOR);
        h >>= 32 - log_buckets;

        // compare and swap
        // search for next bucket until CAS succeeds
        // pre-check for zero key
        while (table[h].key != 0 ||
            !__sync_bool_compare_and_swap(&table[h].key, 0, keys[i])) {
            h = (h + 1) & (buckets - 1);
        }

        // after key set value too
        // this need not be atomic because no other thread can
        // write to this bucket as table[h].key != 0
        table[h].val = vals[i];
  }

  // return number of tuples inserted
  // if required later compare with tuples
  return (end - begin);
}

void flush_item(const aggr_t item,
    const int8_t log_estimate,
    const uint32_t estimate) {

    uint32_t agg_hash, prev;
    agg_hash = item.key * HASH_FACTOR;
    agg_hash >>= 32 - log_estimate;

    // search for empty slot or matching key in global aggregate table
    while(!(aggr_tbl[agg_hash].key == item.key || aggr_tbl[agg_hash].key == 0))
        agg_hash = (agg_hash + 1) & (estimate - 1);

    // aggregation key did not match
    // linear probe the aggregate table for empty slot
    if (!(aggr_tbl[agg_hash].key == item.key)) {
        do {
            prev = __sync_val_compare_and_swap(&aggr_tbl[agg_hash].key, 0,
                    item.key);
            // aggr_key write succeeds or clashes
            if (prev == 0 || prev == item.key)
                break; // out of do-while
            agg_hash = (agg_hash + 1) & (estimate - 1);
        }while(1);
    }
    __sync_add_and_fetch(&aggr_tbl[agg_hash].sum, item.sum);
    __sync_add_and_fetch(&aggr_tbl[agg_hash].count, item.count);
    return;
}

// Called on a portion of outer table
// Compares primary key of outer relation record
// with records inserted in hash table based on
// the multiplicative hash computed
// Returns partial result as result_t
// each query thread calls this method
void update_aggregates(const bucket_t *table,
    const int8_t log_buckets,
    const size_t buckets,
    const size_t outer_beg,
    const size_t outer_end,
    const uint32_t* outer_keys,
    const uint32_t* outer_aggr_keys,
    const int8_t log_estimate,
    const uint32_t* outer_vals) {

    size_t o, h, i, agg_hash;
    uint32_t key, agg_key;
    uint32_t estimate = 1 << log_estimate;
    uint64_t value;
    const int8_t log_entries = 12;
    const uint32_t entries = 1 << log_entries; // cache size

    // allocate local cache
    aggr_t *cache = (aggr_t*)calloc(entries, sizeof(aggr_t));
    assert(cache != NULL);

    //  probe outer table using hash table
    //  outer_beg to outer_end - 1
    for (o = outer_beg; o < outer_end; ++o) {
        key = outer_keys[o];
        //  multiplicative hashing
        h = (uint32_t) (key * HASH_FACTOR);
        h >>= 32 - log_buckets;

        //  search for matching bucket
        while (table[h].key != 0) {
            //  product.id == order.product_id
            if (table[h].key == key) {
                value = (uint64_t)table[h].val * outer_vals[o];
                agg_key = outer_aggr_keys[o];
                agg_hash = (uint32_t)(agg_key * HASH_FACTOR);
                agg_hash >>= 32 - log_entries;

                // cache hit
                if (cache[agg_hash].key == agg_key) {
                    cache[agg_hash].sum += value;
                    cache[agg_hash].count += 1;                    
                }

                // new entry
                else if (cache[agg_hash].key == 0) {
                    cache[agg_hash].key = agg_key;
                    cache[agg_hash].sum = value;
                    cache[agg_hash].count = 1;
                }

                // cache miss flush the current item
                else {
                    flush_item(cache[agg_hash], log_estimate, estimate);
                    cache[agg_hash].key = agg_key;
                    cache[agg_hash].sum = value;
                    cache[agg_hash].count = 1;
                }
                break; // out of outer table probing
            }
            // go to next bucket (linear probing)
            h = (h + 1) & (buckets - 1);
        }
    }

    for (i = 0; i < entries; ++i) {
        if (cache[i].count > 0)
            flush_item(cache[i], log_estimate, estimate);
    }
    free(cache);
    return;
}

// merge results from aggregate table
result_t partial_result(const size_t thread, const size_t threads,
    const int8_t log_estimate){

    uint32_t i;
    uint32_t estimate = 1 << log_estimate;
    result_t result = {0, 0};

    // thread boundaries for outer table
    size_t beg = (estimate / threads) * (thread + 0);
    size_t end = (estimate / threads) * (thread + 1);

    // handle last thread boundary
    if (thread + 1 == threads)
        end = estimate;

    // iterate over portion of aggregate table
    for(i = beg; i < end; i++){
        if (aggr_tbl[i].count > 0){
            result.avg += aggr_tbl[i].sum / aggr_tbl[i].count;
            result.count += 1;
        }
    }
    return result;
}

// allocate memory to aggregate table
int8_t alloc_aggr_tbl(const size_t threads, const size_t thread, 
    const uint32_t partitions, uint32_t **bitmaps){
    uint32_t i, j, estimate = 0;
    int8_t log_estimate = 0;
    // merge other bitmaps into current thread's bitmaps
    for (i = 0; i < threads; i++){
        // already present current thread's bitmaps
        if (i == thread)
            continue;
        for (j = 0; j < partitions; j++)
            bitmaps[thread][j] |= bitmaps[i][j];
    }

    // calculate estimate
    for (j = 0; j < partitions; j++)
        estimate += (1 << count_trailing_zeros(~bitmaps[thread][j]));
    estimate /= PHI;

    // check for power of 2
    if (!(estimate & (estimate - 1)))
        log_estimate = count_trailing_zeros(estimate);
    else{
        // approximate log
        while (estimate > 1){
            log_estimate += 1;
            estimate >>= 1;
        }
        // overestimate a little
        // avoid small sized aggregation table
        log_estimate += 1;
    }

    // allocate table of size 2^log_estimate
    aggr_tbl = (aggr_t*)calloc(1 << log_estimate, sizeof(aggr_t));
    assert(aggr_tbl != NULL);
    return log_estimate;
}

// run each thread
void* q4112_run_thread(void* arg) {
    q4112_run_info_t* info = (q4112_run_info_t*) arg;
    assert(pthread_equal(pthread_self(), info->id));

    int ret;
    result_t result = {0, 0};

    //  copy info
    const size_t thread  = info->thread;
    const size_t threads = info->threads;
    const size_t inner_tuples = info->inner_tuples;
    const size_t outer_tuples = info->outer_tuples;
    const int8_t log_buckets = info->log_buckets;
    const size_t buckets = info->buckets;
    bucket_t* table = info->table;
    pthread_barrier_t *barrier = info->barrier;

    const uint32_t* inner_keys = info->inner_keys;
    const uint32_t* inner_vals = info->inner_vals;
    const uint32_t* outer_keys = info->outer_keys;
    const uint32_t* outer_vals = info->outer_vals;
    const uint32_t* outer_aggr_keys = info->outer_aggr_keys;
    const int8_t log_partitions = info->log_partitions;
    const uint32_t partitions = 1 << log_partitions;
    int8_t *log_estimate = info->log_estimate;

    // corresponding bitmaps for the thread
    uint32_t **bitmaps = info->bitmaps;

    //  thread boundaries for outer table
    size_t outer_beg = (outer_tuples / threads) * (thread + 0);
    size_t outer_end = (outer_tuples / threads) * (thread + 1);

    // thread boundaries for inner table
    size_t inner_beg = (inner_tuples / threads) * (thread + 0);
    size_t inner_end = (inner_tuples / threads) * (thread + 1);

    // handle last thread boundary
    if (thread + 1 == threads) {
        outer_end = outer_tuples;
        inner_end = inner_tuples;
    }

    // get estimate for thread's partition
    // number of log_partitions = number of threads
    _estimate(bitmaps[thread], log_partitions, 
        (outer_aggr_keys + outer_beg), (outer_end - outer_beg));

    // synchronize participating threads for collecting estimates
    ret = pthread_barrier_wait(barrier);
    assert(ret == 0 || ret == PTHREAD_BARRIER_SERIAL_THREAD);

    // last thread does this
    if (ret == PTHREAD_BARRIER_SERIAL_THREAD)
        *log_estimate = alloc_aggr_tbl(threads, thread, partitions, bitmaps);

    // synchronize participating threads for aggergate table allocation
    ret = pthread_barrier_wait(barrier);
    assert(ret == 0 || ret == PTHREAD_BARRIER_SERIAL_THREAD);

    // insert to hash table
    inner_hash_table(table, inner_beg, inner_end, inner_keys,
        inner_vals, log_buckets, buckets);

    // synchronize threads before aggregation
    ret = pthread_barrier_wait(barrier);
    assert(ret == 0 || ret == PTHREAD_BARRIER_SERIAL_THREAD);

    update_aggregates(table, log_buckets, buckets, outer_beg, outer_end,
        outer_keys, outer_aggr_keys, *log_estimate, outer_vals);

    // synchronize to merge results
    ret = pthread_barrier_wait(barrier);
    assert(ret == 0 || ret == PTHREAD_BARRIER_SERIAL_THREAD);

    result = partial_result(thread, threads, *log_estimate);
    // extract query result in thread info
    info->avg = result.avg;
    info->count = result.count;
    pthread_exit(NULL);
}

uint64_t q4112_run(
    const uint32_t* inner_keys,
    const uint32_t* inner_vals,
    size_t inner_tuples,
    const uint32_t* outer_join_keys,
    const uint32_t* outer_aggr_keys,
    const uint32_t* outer_vals,
    size_t outer_tuples,
    int threads) {

    int8_t ret;
    int8_t log_partitions = 12; // optimized for 16KB L1 cache
    uint32_t partitions = 1 << log_partitions;

    // number of buckets for hash table
    int8_t log_buckets = 1;
    size_t buckets = 2;

    //  check number of threads
    int t, max_threads = sysconf(_SC_NPROCESSORS_ONLN);
    assert(max_threads > 0 && threads > 0 && threads <= max_threads);

    // malloc for thread barrier
    pthread_barrier_t *barrier = \
        (pthread_barrier_t*)malloc(sizeof(pthread_barrier_t));
    assert(barrier != NULL);

    // malloc for thread info for all threads
    q4112_run_info_t* info = (q4112_run_info_t*)
        malloc(threads * sizeof(q4112_run_info_t));
    // assert malloc succeeded
    assert(info != NULL);

    // merge them in thread after first barrier
    uint32_t **bitmaps = (uint32_t**)malloc(threads * sizeof(uint32_t*));
    assert(bitmaps != NULL);

    // log of outer aggregate table size
    int8_t *log_estimate = (int8_t*)malloc(sizeof(int8_t));
    assert(log_estimate != NULL);

    // create bitmap arrays
    for (ret = 0; ret < threads; ret++){
        bitmaps[ret] = (uint32_t*)calloc(partitions, sizeof(uint32_t));
    }

    //  set the number of hash table buckets to be 2^k
    //  the hash table fill rate will be between 1/3 and 2/3
    while (buckets * 0.67 < inner_tuples) {
        log_buckets += 1;
        buckets <<= 1;  // double buckets
    }

    // allocate 0 initialized memory for hash buckets
    bucket_t* table = (bucket_t*) calloc(buckets, sizeof(bucket_t));
    assert(table != NULL);

    // initialize thread barrier
    ret = pthread_barrier_init(barrier, NULL, threads);
    assert(ret == 0);

    // create and run threads
    for (t = 0; t < threads; ++t) {
        info[t].thread = t;
        info[t].threads = threads;
        info[t].inner_keys = inner_keys;
        info[t].inner_vals = inner_vals;
        info[t].outer_keys = outer_join_keys;
        info[t].outer_aggr_keys = outer_aggr_keys;
        info[t].outer_vals = outer_vals;
        info[t].inner_tuples = inner_tuples;
        info[t].outer_tuples = outer_tuples;
        info[t].table = table;
        info[t].buckets = buckets;
        info[t].log_buckets = log_buckets;
        info[t].barrier = barrier;
        //Flajolet-Martin estimates
        info[t].log_partitions = log_partitions;
        info[t].bitmaps = bitmaps;  // array of bitmaps
        info[t].log_estimate = log_estimate;
        pthread_create(&info[t].id, NULL, q4112_run_thread, &info[t]);
    }

    //  gather result from all threads
    uint64_t averages = 0;
    uint64_t counts = 0;
    for (t = 0; t != threads; ++t) {
        pthread_join(info[t].id, NULL);
        averages += info[t].avg;
        counts += info[t].count;
    }

    // destroy barrier after threads join
    ret = pthread_barrier_destroy(barrier);
    assert(ret == 0);

    // release memory
    free(log_estimate);
    for (ret = 0; ret < threads; ret++)
        free(bitmaps[ret]);
    free(bitmaps);
    free(aggr_tbl);
    free(info);
    free(table);

    // return average
    return averages / counts;
}
