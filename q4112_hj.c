#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdbool.h>

//hashing factor
#define HASH_FACTOR 0x9e3779b1

//hash bucket struct
typedef struct {
  uint32_t key;
  uint32_t val;
} bucket_t;

//result struct, contains sum and count
//of order values and number of orders
//respectively
typedef struct {
    uint64_t sum;
    uint32_t count;
} result_t;

//thread info struct
typedef struct {
    pthread_t id;
    int thread;
    int threads;
    size_t inner_tuples;
    size_t outer_tuples;
    const uint32_t* inner_keys;
    const uint32_t* inner_vals;
    const uint32_t* outer_keys;
    const uint32_t* outer_vals;
    uint64_t sum;
    uint32_t count;
    //pass bucket info to threads
    int8_t log_buckets;
    size_t buckets;
    bucket_t* table;
    //pass barrier
    pthread_barrier_t *barrier;
} q4112_run_info_t;

//fills given hash table
int inner_hash_table(bucket_t* table,
    const size_t begin,
    const size_t end,
    const uint32_t* keys,
    const uint32_t* vals, 
    const int8_t log_buckets,
    const size_t buckets){

    //iterator and hash key variables
    size_t i, h;

    //build table from begin to end-1
    for (i = begin; i < end; ++i) {

        // multiplicative hashing
        h = (uint32_t) (keys[i] * HASH_FACTOR);
        h >>= 32 - log_buckets;

        //compare and swap
        //search for next bucket until CAS succeeds
        //pre-check for zero key
        while (table[h].key != 0 || 
            !__sync_bool_compare_and_swap(&table[h].key, 0, keys[i])){
            h = (h + 1) & (buckets - 1);
        }

        //after key set value too
        //this need not be atomic because no other thread can
        //write to this bucket as table[h].key != 0
        table[h].val = vals[i];
  }

  //return number of tuples inserted
  //if required later compare with tuples
  return (end - begin);
}

result_t partial_result(const bucket_t *table,
    const int8_t log_buckets,
    const size_t buckets,
    const size_t outer_beg,
    const size_t outer_end,
    const uint32_t* outer_keys,
    const uint32_t* outer_vals){

    size_t o, h;
    result_t result = {0, 0};

    // probe outer table using hash table
    for (o = outer_beg; o < outer_end; ++o) {
        uint32_t key = outer_keys[o];
        // multiplicative hashing
        h = (uint32_t) (key * HASH_FACTOR);
        h >>= 32 - log_buckets;
        // search for matching bucket
        uint32_t tab = table[h].key;
        while (tab != 0) {
            // keys match
            if (tab == key) {
                // update single aggregate
                result.sum += table[h].val * (uint64_t) outer_vals[o];
                result.count += 1;
                // guaranteed single match (join on primary key)
                break;
            }
            // go to next bucket (linear probing)
            h = (h + 1) & (buckets - 1);
            tab = table[h].key;
        }
    }

    //return partial result
    return result;
}

//run each thread
void* q4112_run_thread(void* arg) {
    q4112_run_info_t* info = (q4112_run_info_t*) arg;
    assert(pthread_equal(pthread_self(), info->id));

    // initialize ggregate
    result_t result = {0, 0};

    // copy info
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

    // thread boundaries for outer table
    size_t outer_beg = (outer_tuples / threads) * (thread + 0);
    size_t outer_end = (outer_tuples / threads) * (thread + 1);

    //thread boundaries for inner table
    size_t inner_beg = (inner_tuples / threads) * (thread + 0);
    size_t inner_end = (inner_tuples / threads) * (thread + 1);

    //handle last thread boundary
    if (thread + 1 == threads){
        outer_end = outer_tuples;
        inner_end = inner_tuples;
    }

    //insert to hash table
    inner_hash_table(table, inner_beg, inner_end, inner_keys,
        inner_vals, log_buckets, buckets);

    //synchronize participating threads
    int ret = pthread_barrier_wait(barrier);
    //assert thread woke up with correct return code
    assert(ret == 0 || ret == PTHREAD_BARRIER_SERIAL_THREAD);

    //read and calculate results
    result = partial_result(table, log_buckets, buckets,
        outer_beg, outer_end, outer_keys, outer_vals);

    //extract query result in thread info
    info->sum = result.sum;
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

    int ret;
    //declare thread barrier
    pthread_barrier_t *barrier;

    //number of hash buckets
    int8_t log_buckets = 1;
    size_t buckets = 2;

    // check number of threads
    int t, max_threads = sysconf(_SC_NPROCESSORS_ONLN);
    assert(max_threads > 0 && threads > 0 && threads <= max_threads);

    //malloc for thread barrier
    barrier = (pthread_barrier_t*)malloc(sizeof(pthread_barrier_t));
    assert(barrier != NULL);

    //malloc for thread info
    q4112_run_info_t* info = (q4112_run_info_t*)
        malloc(threads * sizeof(q4112_run_info_t));
  
    //assert malloc succeeded
    assert(info != NULL);
  
    // set the number of hash table buckets to be 2^k
    // the hash table fill rate will be between 1/3 and 2/3
    while (buckets * 0.67 < inner_tuples) {
        log_buckets += 1;
        buckets += buckets;
    }

    //allocate 0 initialized memory for hash buckets
    bucket_t* table = (bucket_t*) calloc(buckets, sizeof(bucket_t));
    assert(table != NULL);

    //initialize thread barrier
    ret = pthread_barrier_init(barrier, NULL, threads);
    assert(ret == 0);

    //create and run threads
    for (t = 0; t != threads; ++t) {
        info[t].thread = t;
        info[t].threads = threads;
        info[t].inner_keys = inner_keys;
        info[t].inner_vals = inner_vals;
        info[t].outer_keys = outer_join_keys;
        info[t].outer_vals = outer_vals;
        info[t].inner_tuples = inner_tuples;
        info[t].outer_tuples = outer_tuples;
        info[t].table = table;
        info[t].buckets = buckets;
        info[t].log_buckets = log_buckets;
        info[t].barrier = barrier;
        pthread_create(&info[t].id, NULL, q4112_run_thread, &info[t]);
    }

    // gather result from all threads
    uint64_t sum = 0;
    uint32_t count = 0;
    for (t = 0; t != threads; ++t) {
        pthread_join(info[t].id, NULL);
        sum += info[t].sum;
        count += info[t].count;
    }

    //destroy barrier after threads join
    ret = pthread_barrier_destroy(barrier);
    assert(ret == 0);

    //release memory
    free(info);
    free(table);
    return sum / count;
}
