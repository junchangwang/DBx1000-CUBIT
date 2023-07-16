### About this project

DBx1000 is a OLTP DBMS written by xxx. The readme file of the original project locates as README-DBx1000.md.

We use DBx1000 as an application of our algorithm, CUBIT. we extend it to support OLAP by adding the following features:
    - TPC-H (locates at benchmarks/tpch_xxx.cpp)
    - Bw-Tree
    - ART

The original code of Bw-Tree and ART locate in [index-microbench](https://github.com/wangziqi2016/index-microbench). We add wrappers for them to integrate them into the DBx1000 project.

### How to compile
    - get the source code of CUBIT from https://anonymous.4open.science/r/CUBIT
    - cd CUBIT && ./build.sh
    - make 

### How to execute
    - ./run_sz_DBx.py