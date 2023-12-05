### About this project

DBx1000 is an academic OLTP DBMS to evaluate the scalability of different layers of DB systems. The original readme file can be found at README-DBx1000.md.

We have integrated CUBIT into DBx1000 on TPC-H. The core code of the evaluation is in benchmarks/tpch_txn.cpp in which we implement Q6, RF1, and RF2.

Furthermore, we add the following indexes according to the Open Bw-Tree project (https://github.com/wangziqi2016/index-microbench).
- Bw-Tree (storage/index_bwtree)
- ART (storage/index_art)

### How to compile
- Retrieve the source code of CUBIT from https://github.com/junchangwang/CUBIT
- Compile CUBIT by using the command "cd CUBIT && ./build.sh"
- Build the DBx1000-CUBIT project by using the command "make"

### How to execute
- ./python3 run_sz_DBx.py
- ./gen_graphs_DBx.sh

