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

// result struct, contains sum of order values
// and number of orders respectively
// typedef struct {
//     uint64_t sum;
//     uint32_t count;
// } result_t;

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
    uint32_t *estimates;
} q4112_run_info_t;

// global aggregate table struct
typedef struct
{
    uint32_t key;   // aggregate key
    uint64_t sum;   // aggregate sum
    uint32_t count; // aggregate count
} aggr_t;

// pointer to global aggregate table
// shared across threads
aggr_t *aggregate_table;


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
void _estimate(uint32_t *estimate, const int8_t log_partitions, 
    const uint32_t* keys, size_t size) {
    uint32_t bitmap = 0;
    size_t i;
    uint32_t h;
    // size_t h, i, partitions = 1 << log_partitions;
    // uint32_t* bitmaps = (uint32_t *)calloc(partitions, sizeof(uint32_t));

    for (i = 0; i < size; i++) {
        h = keys[i] * HASH_FACTOR;  // multiplicative hash
        bitmap |= h & -h;
        // size_t p = h & (partitions - 1);  // use some hash bits to partition
        // h >>= log_partitions;  // use remaining hash bits for the bitmap
        // bitmaps[p] |= h & -h;  // update bitmap of partition
    }

    // free thread local bitmaps
    // free(bitmaps);

    // write to estimate location
    *estimate = (1 << count_trailing_zeros(~bitmap)) / PHI;
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

// Called on a portion of outer table
// Compares primary key of outer relation record
// with records inserted in hash table based on
// the multiplicative hash computed
// Returns partial result as result_t
// each query thread calls this method
// result_t partial_result(const bucket_t *table,
//     const int8_t log_buckets,
//     const size_t buckets,
//     const size_t outer_beg,
//     const size_t outer_end,
//     const uint32_t* outer_keys,
//     const uint32_t* outer_vals) {

//     size_t o, h;
//     // initialize partial result
//     result_t result = {0, 0};

//     //  probe outer table using hash table
//     //  outer_beg to outer_end - 1
//     for (o = outer_beg; o < outer_end; ++o) {
//         uint32_t key = outer_keys[o];
//         //  multiplicative hashing
//         h = (uint32_t) (key * HASH_FACTOR);
//         h >>= 32 - log_buckets;
//         //  search for matching bucket
//         uint32_t tab = table[h].key;
//         while (tab != 0) {
//             //  keys match
//             if (tab == key) {
//                 //  update partial result aggregate
//                 result.sum += table[h].val * (uint64_t) outer_vals[o];
//                 result.count += 1;
//                 //  guaranteed single match (join on primary key)
//                 break;
//             }
//             //  go to next bucket (linear probing)
//             h = (h + 1) & (buckets - 1);
//             tab = table[h].key;
//         }
//     }

//     // return partial result
//     return result;
// }

// run each thread
void* q4112_run_thread(void* arg) {
    q4112_run_info_t* info = (q4112_run_info_t*) arg;
    assert(pthread_equal(pthread_self(), info->id));

    //  initialize ggregate
    // result_t result = {0, 0};

    // iterator
    int8_t i;
    int ret;

    //  copy info
    size_t thread  = info->thread;
    size_t threads = info->threads;
    size_t inner_tuples = info->inner_tuples;
    size_t outer_tuples = info->outer_tuples;
    int8_t log_buckets = info->log_buckets;
    size_t buckets = info->buckets;
    bucket_t* table = info->table;
    pthread_barrier_t *barrier = info->barrier;

    const uint32_t* inner_keys = info->inner_keys;
    const uint32_t* inner_vals = info->inner_vals;
    const uint32_t* outer_keys = info->outer_keys;
    const uint32_t* outer_vals = info->outer_vals;
    const uint32_t* outer_aggr_keys = info->outer_aggr_keys;

    // estimates array
    uint32_t *estimates = info->estimates;

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
    _estimate(&estimates[thread], (int8_t)threads, 
        (outer_aggr_keys + outer_beg), (outer_end - outer_beg));

    // synchronize participating threads for collecting estimates
    ret = pthread_barrier_wait(barrier);
    assert(ret == 0 || ret == PTHREAD_BARRIER_SERIAL_THREAD);

    if (ret == PTHREAD_BARRIER_SERIAL_THREAD){
        uint32_t final_estimate = 0;
        for (i = 0; i < threads; i++)
            final_estimate += estimates[i];
        printf("final: %u\n", final_estimate);
    }

    // synchronize participating threads for collecting estimates
    ret = pthread_barrier_wait(barrier);
    assert(ret == 0 || ret == PTHREAD_BARRIER_SERIAL_THREAD);

    // insert to hash table
    // inner_hash_table(table, inner_beg, inner_end, inner_keys,
    //     inner_vals, log_buckets, buckets);

    // read and calculate results
    // result = partial_result(table, log_buckets, buckets,
    //     outer_beg, outer_end, outer_keys, outer_vals);

    // extract query result in thread info
    // info->sum = result.sum;
    // info->count = result.count;
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
    uint32_t *estimates = (uint32_t*)malloc(threads * sizeof(uint32_t));
    assert(estimates != NULL);

    //  set the number of hash table buckets to be 2^k
    //  the hash table fill rate will be between 1/3 and 2/3
    while (buckets * 0.67 < inner_tuples) {
        log_buckets += 1;
        buckets += buckets;
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
        info[t].estimates = estimates;
        pthread_create(&info[t].id, NULL, q4112_run_thread, &info[t]);
    }

    //  gather result from all threads
    uint64_t sum = 0;
    uint32_t count = 0;
    for (t = 0; t != threads; ++t) {
        pthread_join(info[t].id, NULL);
    }

    // // estimate of aggergate keys
    // sum = 0;
    // for (t = 0; t < (1<<12); ++t)
    //     sum += ((size_t) 1) << count_trailing_zeros(~bitmaps[t]);
    // sum /= PHI;

    // destroy barrier after threads join
    ret = pthread_barrier_destroy(barrier);
    assert(ret == 0);

    // release memory
    free(estimates);
    free(info);
    free(table);

    // return average
    return sum / count;
}
