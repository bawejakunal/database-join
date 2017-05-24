# Database In-Memory join and Aggregate Queries

This repository demonstrates in-memory joins and aggregate query
implementations for a general database, in C.

This project was done as part of COMS W4112 Database Systems Implementation
for Fall 2017 at Columbia University.


### Source description

1. Problem statement: [4112_project_2.pdf](4112_project_2.pdf)
2. q4112_main.c:   Project API
3. q4112_nlj_1.c:  single-threaded nested loop join
4. q4112_nlj.c:    multi-threaded nested loop join
5. q4112_hj_1.c:   single-threaded hash join
6. q4112_hj.c:     multi-threaded hash join
7. q4112.c:        aggregation query (described in Problem Statement)


### Compilation

    1. `make clean`
    2. `make all`

**NOTE**: DO NOT DELETE [q4112_gen.o](q4112_gen.o). It implements in-memory
      data generation for database queries and the source file is 
      unavailable.

### Run this project

Each executable compiled above, runs a given `configuration` for 5 repititions
as and logs the observations into a csv file.

The executables take in a number of optional arguments (in order):
1. inner_tuples - number of tuples in inner relation table (default: 1000)
2. inner_selectivity - fraction of tuples that satisfy query (default: 1.0)
                     - this must be in range `[0-1]`
3. inner_val_max - max value of inner relation field (default: 10000000)
                     - used for data generation in q4112_gen.o
4. outer_tuples - number of tuples in outer table (default: 1000000)
5. outer_selectivity - fraction of tuples that satisfy query (default: 1.0)
                     - this must be in range `[0-1]`
6. outer_val_max - max value of outer relation field (default: 1000)
7. groups - number of aggregation groups (group by query)
8. hh_groups - number of hard hitter groups
             - tests for memory access contention in multi-threaded code
9. hh_probability - fraction of groups that are hard hitter (default: 0)
10. threads - number of threads to be used by query (default: 1)
11. res_file - result csv format file (default: q4112.csv)
             - results of successive runs/configurations are appended

**Example-1**

`./q4112_hj 100 1.0 99999 1000000000 0.5 99999 0 0 0.0 16 q4112_hj.csv`
1. inner_tuples: 100
2. inner_selectivity: 1.0
3. inner_val_max: 99999
4. outer_tuples: 1000000000
5. outer_selectivity: 0.5
6. outer_val_max: 99999
7. groups: 0
8. hh_groups: 0
9. hh_probability: 0
10. threads: 16
11. res_file: q4112_hj.csv

**Example-2**

`/q4112 100 1.0 99999 1000000000 1.0 99999 100000000 100 0.5 16 q4112.csv`
1. inner_tuples: 100
2. inner_selectivity: 1.0
3. inner_val_max: 99999
4. outer_tuples: 1000000000
5. outer_selectivity: 1.0
6. outer_val_max: 99999
7. groups: 100000000
8. hh_groups: 100
9. hh_probability: 0.5
10. threads: 16
11. res_file: q4112.csv
