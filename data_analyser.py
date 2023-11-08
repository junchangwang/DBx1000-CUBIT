
from calendar import c
from multiprocessing.pool import ApplyResult
from numbers import Rational
from operator import index
import queue
from re import A, U
import shutil
import sys
import os


ROOT_PATH = os.getcwd()

ossystem = os.system

core_number = [1, 2, 4, 8, 16, 24, 32]

###################################### cmd #########################################

def latency_analysis(filename):
    f = open(filename)
    # Svec = [] # scan     
    Hash = [] # hash
    BTree = [] # btree
    BwTree = []
    ART = []
    CUBIT = [] # cubit

    ret = []

    for line in f:
        a = line.split()
        if len(a) != 5:
            continue
        elif line.startswith('Hash '):
            Hash.append(float(a[-3]))
        elif line.startswith('BTree '):
            BTree.append(float(a[-3]))
        elif line.startswith('BWTree '):
            BwTree.append(float(a[-3]))
        elif line.startswith('ART '):
            ART.append(float(a[-3]))
        elif line.startswith('CUBIT '):
            CUBIT.append(float(a[-3]))
        else:
            continue
    if len(Hash) != 0:
        ret.append(sum(Hash) / len(Hash)) 
    else:
        ret.append(0)
    if len(BTree) != 0:
        ret.append(sum(BTree) / len(BTree)) 
    else:
        ret.append(0)
    if len(BwTree) != 0:
        ret.append(sum(BwTree) / len(BwTree)) 
    else:
        ret.append(0)
    if len(ART) != 0:
        ret.append(sum(ART) / len(ART)) 
    else:
        ret.append(0)
    if len(CUBIT) != 0:
        ret.append(sum(CUBIT) / len(CUBIT)) 
    else:
        ret.append(0)
    return ret

def throughput_analysis(filename):
    f = open(filename)
    ret = []
    
    for line in f:
        a = line.split()
        if line.startswith('ThroughputScan '):
            ret.append(float(a[-1]))
        elif line.startswith('ThroughputHash '):
            ret.append(float(a[-1]))
        elif line.startswith('ThroughputBTree '):
            ret.append(float(a[-1]))
        elif line.startswith('ThroughputBWTree '):
            ret.append(float(a[-1]))
        elif line.startswith('ThroughputART '):
            ret.append(float(a[-1]))
        elif line.startswith('ThroughputCUBIT '):
            ret.append(float(a[-1]))
        else:
            continue
    return ret

def memory_analysis(filename):
    f = open(filename)
    ret = []

    for line in f:
        a = line.split()
        if line.startswith('HashMemory '):
            ret.append(float(a[-1]))
        elif line.startswith('BtreeMemory '):
            ret.append(float(a[-1]))
        elif line.startswith('BwtreeMemory '):
            ret.append(float(a[-1]))
        elif line.startswith('ARTMemory '):
            ret.append(float(a[-1]))
        elif line.startswith('CUBITMemory '):
            ret.append(float(a[-1]))
        else:
            continue

    return ret

def index_time_analysis(filename):
    f = open(filename)  
    Hash_tuple = [] # hash
    Btree_tuple = [] # btree
    Bwtree_tuple = [] # bwtree
    ART_tuple = [] # ART
    CUBIT_tuple = [] # cubit

    Hash_index = [] 
    Btree_index = [] 
    Bwtree_index = [] 
    ART_index = [] 
    CUBIT_index = [] 

    ret = []

    for line in f:
        a = line.split()
        if len(a) != 5:
            continue
        elif line.startswith('Hash '):
            Hash_tuple.append(float(a[-1]))
            Hash_index.append(float(a[-2]))
        elif line.startswith('BTree '):
            Btree_tuple.append(float(a[-1]))
            Btree_index.append(float(a[-2]))
        elif line.startswith('BWTree '):
            Bwtree_tuple.append(float(a[-1]))
            Bwtree_index.append(float(a[-2]))
        elif line.startswith('ART '):
            ART_tuple.append(float(a[-1]))
            ART_index.append(float(a[-2]))
        elif line.startswith('CUBIT '):
            CUBIT_tuple.append(float(a[-1]))
            CUBIT_index.append(float(a[-2]))
        else:
            continue

    if len(Hash_index) != 0:
        ret.append(sum(Hash_index) / len(Hash_index)) 
    else:
        ret.append(0)
    if len(Hash_tuple) != 0:
        ret.append(sum(Hash_tuple) / len(Hash_tuple)) 
    else:
        ret.append(0)

    if len(Btree_index) != 0:
        ret.append(sum(Btree_index) / len(Btree_index)) 
    else:
        ret.append(0)
    if len(Btree_tuple) != 0:
        ret.append(sum(Btree_tuple) / len(Btree_tuple)) 
    else:
        ret.append(0)

    if len(Bwtree_index) != 0:
        ret.append(sum(Bwtree_index) / len(Bwtree_index)) 
    else:
        ret.append(0)
    if len(Bwtree_tuple) != 0:
        ret.append(sum(Bwtree_tuple) / len(Bwtree_tuple)) 
    else:
        ret.append(0)

    if len(ART_index) != 0:
        ret.append(sum(ART_index) / len(ART_index)) 
    else:
        ret.append(0)
    if len(ART_tuple) != 0:
        ret.append(sum(ART_tuple) / len(ART_tuple)) 
    else:
        ret.append(0)

    if len(CUBIT_index) != 0:
        ret.append(sum(CUBIT_index) / len(CUBIT_index)) 
    else:
        ret.append(0)
    if len(CUBIT_tuple) != 0:
        ret.append(sum(CUBIT_tuple) / len(CUBIT_tuple)) 
    else:
        ret.append(0)

    return ret

def  run_indexAndTuples():
    f = open('dat_DBx/index_time.dat', 'w')
    ret = [[]]
    ret.clear()
    use_number = [1, 4, 16, 32]
    for num in use_number:
        ret.append(index_time_analysis('dat_tmp_DBx/core_{}.dat'.format(num)))

    pos = 1
    for run in [0,1,2,3]:
        f.write('{0}  {1}  {2} "Hash"\n'.format(pos, ret[run][0], ret[run][1]))
        pos += 1
        f.write('{0}  {1}  {2} "B^+-Tree"\n'.format(pos, ret[run][2], ret[run][3]))
        pos += 1
        f.write('{0}  {1}  {2} "Bw-Tree"\n'.format(pos, ret[run][4], ret[run][5]))
        pos += 1
        f.write('{0}  {1}  {2} "ART"\n'.format(pos, ret[run][6], ret[run][7]))
        pos += 1
        f.write('{0}  {1}  {2} "CUBIT"\n'.format(pos, ret[run][8], ret[run][9]))
        pos += 1
        f.write('{0}  0    0 ""\n'.format(pos))
        pos += 1


#     f.write('1  {0}  {1} "Hash"\n'.format(ret[0][0], ret[0][1]))
#     f.write('2  {0}  {1} "B^+-Tree"\n'.format(ret[0][2], ret[0][3]))
#     f.write('3  {0}  {1} "CUBIT"\n'.format(ret[0][4], ret[0][5]))
#     f.write('4  0         0 ""\n')
#     f.write('5  {0}  {1} "Hash"\n'.format(ret[1][0], ret[1][1]))
#     f.write('6  {0}  {1} "B^+-Tree"\n'.format(ret[1][2], ret[1][3]))
#     f.write('7  {0}  {1} "CUBIT"\n'.format(ret[1][4], ret[1][5]))
#     f.write('8  0         0 ""\n')
#     f.write('9  {0}  {1} "Hash"\n'.format(ret[2][0], ret[2][1]))
#     f.write('10  {0}  {1} "B^+-Tree"\n'.format(ret[2][2], ret[2][3]))
#     f.write('11  {0}  {1} "CUBIT"\n'.format(ret[2][4], ret[2][5]))
#     f.write('12  0         0 ""\n')
#     f.write('13  {0}  {1} "Hash"\n'.format(ret[3][0], ret[3][1]))
#     f.write('14  {0}  {1} "B^+-Tree"\n'.format(ret[3][2], ret[3][3]))
#     f.write('15  {0}  {1} "CUBIT"\n'.format(ret[3][4], ret[3][5]))
#     f.write('16  0         0 ""\n')
#     f.write('17  {0}  {1} "Hash"\n'.format(ret[4][0], ret[4][1]))
#     f.write('18  {0}  {1} "B^+-Tree"\n'.format(ret[4][2], ret[4][3]))
#     f.write('19  {0}  {1} "CUBIT"\n'.format(ret[4][4], ret[4][5]))
#     f.write('20  0         0 ""\n')
#     f.write('21  {0}  {1} "Hash"\n'.format(ret[5][0], ret[5][1]))
#     f.write('22  {0}  {1} "B^+-Tree"\n'.format(ret[5][2], ret[5][3]))
#     f.write('23  {0}  {1} "CUBIT"\n'.format(ret[5][4], ret[5][5]))
#     f.write('24  0         0 ""\n')
#     f.write('25  {0}  {1} "Hash"\n'.format(ret[6][0], ret[6][1]))
#     f.write('26  {0}  {1} "B^+-Tree"\n'.format(ret[6][2], ret[6][3]))
#     f.write('27  {0}  {1} "CUBIT"\n'.format(ret[6][4], ret[6][5]))

    f.close()    


def  run_latency():
    f = open('dat_DBx/run_time.dat', 'w')
    ret = [[]]
    ret.clear()
    for num in core_number:
        ret.append(latency_analysis('dat_tmp_DBx/core_{}.dat'.format(num)))

    pos = 1
    for run in [0,1,2,3]:
        f.write('{0}  {1}  "Hash"\n'.format(pos, ret[run][0]))
        pos += 1
        f.write('{0}  {1}  "B^+-Tree"\n'.format(pos, ret[run][1]))
        pos += 1
        f.write('{0}  {1}  "Bw+-Tree"\n'.format(pos, ret[run][2]))
        pos += 1
        f.write('{0}  {1}  "ART"\n'.format(pos, ret[run][3]))
        pos += 1
        f.write('{0}  {1}  "CUBIT"\n'.format(pos, ret[run][4]))
        pos += 2

    # f.write('1  {0}  "Hash"\n'.format(ret[0][0]))
    # f.write('2  {0}  "B^+-Tree"\n'.format(ret[0][1]))
    # f.write('3  {0}  "CUBIT"\n'.format(ret[0][2]))

    # f.write('5  {0}  "Hash"\n'.format(ret[1][0]))
    # f.write('6  {0}  "B^+-Tree"\n'.format(ret[1][1]))
    # f.write('7  {0}  "CUBIT"\n'.format(ret[1][2]))

    # f.write('9  {0}  "Hash"\n'.format(ret[2][0]))
    # f.write('10  {0}  "B^+-Tree"\n'.format(ret[2][1]))
    # f.write('11  {0}  "CUBIT"\n'.format(ret[2][2]))

    # f.write('13  {0}  "Hash"\n'.format(ret[3][0]))
    # f.write('14  {0}  "B^+-Tree"\n'.format(ret[3][1]))
    # f.write('15  {0}  "CUBIT"\n'.format(ret[3][2]))

    # f.write('17  {0}  "Hash"\n'.format(ret[4][0]))
    # f.write('18  {0}  "B^+-Tree"\n'.format(ret[4][1]))
    # f.write('19  {0}  "CUBIT"\n'.format(ret[4][2]))

    # f.write('21  {0}  "Hash"\n'.format(ret[5][0]))
    # f.write('22  {0}  "B^+-Tree"\n'.format(ret[5][1]))
    # f.write('23  {0}  "CUBIT"\n'.format(ret[5][2]))

    # f.write('25  {0}  "Hash"\n'.format(ret[6][0]))
    # f.write('26  {0}  "B^+-Tree"\n'.format(ret[6][1]))
    # f.write('27  {0}  "CUBIT"\n'.format(ret[6][2]))

    f.close()  

def gen_dat_DBx():
    print ('DBx1000 core')
    print ('-' * 10)
    # core_number = [1, 2, 4]
    f = open('dat_DBx/core.dat','w')
    for num in core_number:
        res = throughput_analysis('dat_tmp_DBx/core_{}.dat'.format(num))
        print(res)
        print('\n')
        for tp in res:
            f.write('{} {} \n'.format(num, tp))
    f.close()

    # print ('-' * 10)
    # f = open('dat_DBx/core_f.dat','w')
    # for num in core_number:
    #     res = latency_f_analysis('dat_tmp_DBx/core_{}.dat'.format(num))
    #     print(res)
    #     print('\n')
    #     for tp in res:
    #         f.write('{} {} \n'.format(num, tp))
    # f.close()  

    print ('DBx1000 memory')
    print ('-' * 10)
    f = open('dat_DBx/memory.dat','w')

    res = memory_analysis('dat_tmp_DBx/core_{}.dat'.format(1))
    print(res)
    print('\n')

    cnt = 0
    for tp in res:
        f.write('{} {} \n'.format(cnt, tp))
        cnt += 1

    f.close()  

    print ('-' * 10)
    # index and tuples
    run_indexAndTuples()
    
    print ('-' * 10)
    # run time
    run_latency()

def gen_graph():
    os.chdir("gnuplot-scripts")
    os.system("make make_dir_DBx")
    os.system("make figure_DBx_core")
    os.chdir("../graphs_DBx")
    os.system('echo "Figures generated in \"`pwd`\""')

    #cdf
    os.chdir("../")
    os.system('python3 cdf_sz.py > graphs_DBx/cdf_output')


def main():
    if len(sys.argv) < 2:
        print ("Must specify the directory containing experimental results.")
    else:
        print ("[data_analyser] To process director ", sys.argv[1])

    copy = False
    if os.path.exists('dat_DBx'):
        print ("dat_DBx already exists. Skip building.")
    else:
        os.system("mkdir dat_DBx")
        gen_dat_DBx()
        copy = True

    gen_graph()

    if copy:
        cmd = "cp -r dat_DBx " + sys.argv[1] + "/eva_data/"
        print (cmd)
        os.system(cmd)
    
    print('[data_analyser] Done!\n')

if __name__ == '__main__':
    main()
