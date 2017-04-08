#include <assert.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct {
  uint32_t key;
  uint32_t val;
} bucket_t;

uint64_t q4112_run(
    const uint32_t* inner_keys,
    const uint32_t* inner_vals,
    size_t inner_tuples,
    const uint32_t* outer_join_keys,
    const uint32_t* outer_aggr_keys,
    const uint32_t* outer_vals,
    size_t outer_tuples,
    int threads) {
  assert(threads == 1);
  // set the number of hash table buckets to be 2^k
  // the hash table fill rate will be between 1/3 and 2/3
  int8_t log_buckets = 1;
  size_t buckets = 2;
  while (buckets * 0.67 < inner_tuples) {
    log_buckets += 1;
    buckets += buckets;
  }
  // allocate and initialize the hash table
  // there are no 0 keys (see header) so we use 0 for "no key"
  bucket_t* table = (bucket_t*) calloc(buckets, sizeof(bucket_t));
  assert(table != NULL);
  // build inner table into hash table
  size_t i, o, h;
  for (i = 0; i != inner_tuples; ++i) {
    uint32_t key = inner_keys[i];
    uint32_t val = inner_vals[i];
    // multiplicative hashing
    h = (uint32_t) (key * 0x9e3779b1);
    h >>= 32 - log_buckets;
    // search for empty bucket
    while (table[h].key != 0) {
      // go to next bucket (linear probing)
      h = (h + 1) & (buckets - 1);
    }
    // set bucket
    table[h].key = key;
    table[h].val = val;
  }
  // initialize single aggregate
  uint32_t count = 0;
  uint64_t sum = 0;
  // probe outer table using hash table
  for (o = 0; o != outer_tuples; ++o) {
    uint32_t key = outer_join_keys[o];
    // multiplicative hashing
    h = (uint32_t) (key * 0x9e3779b1);
    h >>= 32 - log_buckets;
    // search for matching bucket
    uint32_t tab = table[h].key;
    while (tab != 0) {
      // keys match
      if (tab == key) {
        // update single aggregate
        sum += table[h].val * (uint64_t) outer_vals[o];
        count += 1;
        // guaranteed single match (join on primary key)
        break;
      }
      // go to next bucket (linear probing)
      h = (h + 1) & (buckets - 1);
      tab = table[h].key;
    }
  }
  // cleanup and return average (integer division)
  free(table);
  return sum / count;
}
