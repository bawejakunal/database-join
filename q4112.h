#ifndef _Q4112_
#define _Q4112_

#include <stdint.h>
#include <stdlib.h>

// used to link q4112_gen() with C++
#ifdef __cplusplus
extern "C"
#endif

// generate data for query
uint64_t q4112_gen(
    // column items.id (primary key, key never 0)
    uint32_t* inner_keys,
    // column items.price
    uint32_t* inner_vals,
    // tuples for table items
    size_t inner_tuples,
    // probability that items.id exists in orders
    double inner_selectivity,
    // max value for items.price
    uint32_t inner_val_max,
    // column orders.item_id (foreign key, key never 0)
    uint32_t* outer_join_keys,
    // column orders.store_id (key never 0, array must be NULL if groups is 0)
    uint32_t* outer_aggr_keys,
    // column orders.quantity
    uint32_t* outer_vals,
    // tuples for table orders
    size_t outer_tuples,
    // probability that orders.item_id exists in items
    double outer_selectivity,
    // max value for orders.quantity
    uint32_t outer_val_max,
    // distinct values for orders.store_id (0 to disable)
    size_t groups,
    // heavy hitter values for orders.store_id
    size_t heavy_hitter_groups,
    // orders.store_id is HH (after all orders.store_id values appear once)
    double heavy_hitter_probability);

// execute query
uint64_t q4112_run(
    // column items.id
    const uint32_t* inner_keys,
    // column items.price
    const uint32_t* inner_vals,
    // tuples for table item
    size_t inner_tuples,
    // column orders.item_id
    const uint32_t* outer_join_keys,
    // column orders.store_id
    const uint32_t* outer_aggr_keys,
    // column orders.quantity
    const uint32_t* outer_vals,
    // tuples for table orders
    size_t outer_tuples,
    // number of threads to use (must not exceed hardware threads)
    int threads);

#endif