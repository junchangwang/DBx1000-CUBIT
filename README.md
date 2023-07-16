### About this project

DBx1000 is an academic OLTP DBMS to evaluate the scalability of different layers of DB systems. The original readme file can be found at README-DBx1000.md.

We use DBx1000 as an application of our updatable bitmap index, CUBIT. We extend DBx1000 to support OLAP by adding the following features:
    - TPC-H (benchmarks/tpch)
    - Bw-Tree (storage/index_bwtree)
    - ART (storage/index_art)

We reuse the Bw-Tree and ART implementation in the Open Bw-Tree project(https://github.com/wangziqi2016/index-microbench). We integrate the code into the DBx1000 project.

### How to compile
    - Retrieve the source code of CUBIT from https://anonymous.4open.science/r/CUBIT
    - Compile CUBIT by using the command "cd CUBIT && ./build.sh"
    - Build the DBx1000-CUBIT project by using the command "make"

### How to execute
    - ./python3 run_sz_DBx.py
    - ./gen_graphs_DBx.sh

