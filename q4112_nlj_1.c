#include <assert.h>
#include <stdint.h>
#include <stdlib.h>

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
  uint64_t sum = 0;
  uint32_t count = 0;
  size_t i, o;
  for (o = 0; o != outer_tuples; ++o) {
    for (i = 0; i != inner_tuples; ++i) {
      if (inner_keys[i] == outer_join_keys[o]) {
        sum += inner_vals[i] * (uint64_t) outer_vals[o];
        count += 1;
        break;
      }
    }
  }
  return sum / count;
}
