#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

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
} q4112_run_info_t;

void* q4112_run_thread(void* arg) {
  q4112_run_info_t* info = (q4112_run_info_t*) arg;
  assert(pthread_equal(pthread_self(), info->id));
  // copy info
  size_t thread  = info->thread;
  size_t threads = info->threads;
  size_t inner_tuples = info->inner_tuples;
  size_t outer_tuples = info->outer_tuples;
  const uint32_t* inner_keys = info->inner_keys;
  const uint32_t* inner_vals = info->inner_vals;
  const uint32_t* outer_keys = info->outer_keys;
  const uint32_t* outer_vals = info->outer_vals;
  // thread boundaries for outer table
  size_t outer_beg = (outer_tuples / threads) * (thread + 0);
  size_t outer_end = (outer_tuples / threads) * (thread + 1);
  if (thread + 1 == threads) outer_end = outer_tuples;
  // initialize aggregate
  uint64_t sum = 0;
  uint32_t count = 0;
  // scan whole inner table but split outer table
  size_t i, o;
  for (o = outer_beg; o != outer_end; ++o) {
    for (i = 0; i != inner_tuples; ++i) {
      if (inner_keys[i] == outer_keys[o]) {
        sum += inner_vals[i] * (uint64_t) outer_vals[o];
        count += 1;
      }
    }
  }
  info->sum = sum;
  info->count = count;
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
  // check number of threads
  int t, max_threads = sysconf(_SC_NPROCESSORS_ONLN);
  assert(max_threads > 0 && threads > 0 && threads <= max_threads);
  // run threads
  q4112_run_info_t* info = (q4112_run_info_t*)
      malloc(threads * sizeof(q4112_run_info_t));
  assert(info != NULL);
  for (t = 0; t != threads; ++t) {
    info[t].thread = t;
    info[t].threads = threads;
    info[t].inner_keys = inner_keys;
    info[t].inner_vals = inner_vals;
    info[t].outer_keys = outer_join_keys;
    info[t].outer_vals = outer_vals;
    info[t].inner_tuples = inner_tuples;
    info[t].outer_tuples = outer_tuples;
    pthread_create(&info[t].id, NULL, q4112_run_thread, &info[t]);
  }
  // gather result
  uint64_t sum = 0;
  uint32_t count = 0;
  for (t = 0; t != threads; ++t) {
    pthread_join(info[t].id, NULL);
    sum += info[t].sum;
    count += info[t].count;
  }
  free(info);
  return sum / count;
}
