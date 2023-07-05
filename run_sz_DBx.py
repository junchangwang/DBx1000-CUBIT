
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

# core_number = [1, 2, 4, 8, 16, 24, 32]
core_number = [1, 2, 4]


###################################### cmd #########################################
def build():      
    os.system("./rundb -M build")


def gen_raw_data(): 
    for num in core_number:
        cmd = './rundb -t{} -M cache > dat_tmp_DBx/core_{}.dat'.format(num, num)
        os.system(cmd)
        print ('Data file dat_tmp_DBx/core_{}.dat has been generated.'.format(num))

def latency_analysis(filename):
    f = open(filename)
    # Svec = [] # scan     
    Hvec = [] # hash
    Bvec = [] # btree
    Cvec = [] # cubit

    ret = []

    for line in f:
        a = line.split()
        if len(a) != 5:
            continue
        elif line.startswith('Hash '):
            Hvec.append(float(a[-3]))
        elif line.startswith('BTree '):
            Bvec.append(float(a[-3]))
        elif line.startswith('CUBIT '):
            Cvec.append(float(a[-3]))
        else:
            continue
    if len(Hvec) != 0:
        ret.append(sum(Hvec) / len(Hvec)) 
    else:
        ret.append(0)
    if len(Bvec) != 0:
        ret.append(sum(Bvec) / len(Bvec)) 
    else:
        ret.append(0)
    if len(Cvec) != 0:
        ret.append(sum(Cvec) / len(Cvec)) 
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
        elif line.startswith('ThroughputCUBIT '):
            ret.append(float(a[-1]))
        else:
            continue
    return ret

def memory_analysis(filename):
    f = open(filename)
    ret = []
    bitmap_memory = 0
    
    for line in f:
        a = line.split()
        if line.startswith('M '): # bitmaps
            bitmap_memory +=float(a[-1])
        elif line.startswith('HashMemory: '):
            ret.append(float(a[-1]))
        elif line.startswith('BtreeMemory: '):
            ret.append(float(a[-1]))
        else:
            continue
    ret.append(bitmap_memory)
    return ret

def index_time_analysis(filename):
    f = open(filename)  
    Hvec = [] # hash
    Bvec = [] # btree
    Cvec = [] # cubit

    Hvec_f = [] # hash, index time
    Bvec_f = [] # btree
    Cvec_f = [] # cubit

    ret = []

    for line in f:
        a = line.split()
        if len(a) != 5:
            continue
        elif line.startswith('Hash '):
            Hvec.append(float(a[-1]))
            Hvec_f.append(float(a[-2]))
        elif line.startswith('BTree '):
            Bvec.append(float(a[-1]))
            Bvec_f.append(float(a[-2]))
        elif line.startswith('CUBIT '):
            Cvec.append(float(a[-1]))
            Cvec_f.append(float(a[-2]))
        else:
            continue

    if len(Hvec_f) != 0:
        ret.append(sum(Hvec_f) / len(Hvec_f)) 
    else:
        ret.append(0)
    if len(Hvec) != 0:
        ret.append(sum(Hvec) / len(Hvec)) 
    else:
        ret.append(0)

    if len(Bvec_f) != 0:
        ret.append(sum(Bvec_f) / len(Bvec_f)) 
    else:
        ret.append(0)
    if len(Bvec) != 0:
        ret.append(sum(Bvec) / len(Bvec)) 
    else:
        ret.append(0)

    if len(Cvec_f) != 0:
        ret.append(sum(Cvec_f) / len(Cvec_f)) 
    else:
        ret.append(0)
    if len(Cvec) != 0:
        ret.append(sum(Cvec) / len(Cvec)) 
    else:
        ret.append(0)
    return ret

def  run_indexAndTuples():
    f = open('dat_DBx/index_time.dat', 'w')
    ret = [[]]
    ret.clear()
    for num in core_number:
        ret.append(index_time_analysis('dat_tmp_DBx/core_{}.dat'.format(num)))

    f.write('1  {0}  {1} "Hash"\n'.format(ret[0][0], ret[0][1]))
    f.write('2  {0}  {1} "B^+-Tree"\n'.format(ret[0][2], ret[0][3]))
    f.write('3  {0}  {1} "CUBIT"\n'.format(ret[0][4], ret[0][5]))
    f.write('4  0         0 ""\n')
    f.write('5  {0}  {1} "Hash"\n'.format(ret[1][0], ret[1][1]))
    f.write('6  {0}  {1} "B^+-Tree"\n'.format(ret[1][2], ret[1][3]))
    f.write('7  {0}  {1} "CUBIT"\n'.format(ret[1][4], ret[1][5]))
    f.write('8  0         0 ""\n')
    f.write('9  {0}  {1} "Hash"\n'.format(ret[2][0], ret[2][1]))
    f.write('10  {0}  {1} "B^+-Tree"\n'.format(ret[2][2], ret[2][3]))
    f.write('11  {0}  {1} "CUBIT"\n'.format(ret[2][4], ret[2][5]))
    f.write('12  0         0 ""\n')
    f.write('13  {0}  {1} "Hash"\n'.format(ret[3][0], ret[3][1]))
    f.write('14  {0}  {1} "B^+-Tree"\n'.format(ret[3][2], ret[3][3]))
    f.write('15  {0}  {1} "CUBIT"\n'.format(ret[3][4], ret[3][5]))
    f.write('16  0         0 ""\n')
    f.write('17  {0}  {1} "Hash"\n'.format(ret[4][0], ret[4][1]))
    f.write('18  {0}  {1} "B^+-Tree"\n'.format(ret[4][2], ret[4][3]))
    f.write('19  {0}  {1} "CUBIT"\n'.format(ret[4][4], ret[4][5]))
    f.write('20  0         0 ""\n')
    f.write('21  {0}  {1} "Hash"\n'.format(ret[5][0], ret[5][1]))
    f.write('22  {0}  {1} "B^+-Tree"\n'.format(ret[5][2], ret[5][3]))
    f.write('23  {0}  {1} "CUBIT"\n'.format(ret[5][4], ret[5][5]))
    f.write('24  0         0 ""\n')
    f.write('25  {0}  {1} "Hash"\n'.format(ret[6][0], ret[6][1]))
    f.write('26  {0}  {1} "B^+-Tree"\n'.format(ret[6][2], ret[6][3]))
    f.write('27  {0}  {1} "CUBIT"\n'.format(ret[6][4], ret[6][5]))
    f.close()    


def  run_latency():
    f = open('dat_DBx/run_time.dat', 'w')
    ret = [[]]
    ret.clear()
    for num in core_number:
        ret.append(latency_analysis('dat_tmp_DBx/core_{}.dat'.format(num)))

    f.write('1  {0}  "Hash"\n'.format(ret[0][0]))
    f.write('2  {0}  "B^+-Tree"\n'.format(ret[0][1]))
    f.write('3  {0}  "CUBIT"\n'.format(ret[0][2]))

    f.write('5  {0}  "Hash"\n'.format(ret[1][0]))
    f.write('6  {0}  "B^+-Tree"\n'.format(ret[1][1]))
    f.write('7  {0}  "CUBIT"\n'.format(ret[1][2]))

    f.write('9  {0}  "Hash"\n'.format(ret[2][0]))
    f.write('10  {0}  "B^+-Tree"\n'.format(ret[2][1]))
    f.write('11  {0}  "CUBIT"\n'.format(ret[2][2]))

    f.write('13  {0}  "Hash"\n'.format(ret[3][0]))
    f.write('14  {0}  "B^+-Tree"\n'.format(ret[3][1]))
    f.write('15  {0}  "CUBIT"\n'.format(ret[3][2]))

    f.write('17  {0}  "Hash"\n'.format(ret[4][0]))
    f.write('18  {0}  "B^+-Tree"\n'.format(ret[4][1]))
    f.write('19  {0}  "CUBIT"\n'.format(ret[4][2]))

    f.write('21  {0}  "Hash"\n'.format(ret[5][0]))
    f.write('22  {0}  "B^+-Tree"\n'.format(ret[5][1]))
    f.write('23  {0}  "CUBIT"\n'.format(ret[5][2]))

    f.write('25  {0}  "Hash"\n'.format(ret[6][0]))
    f.write('26  {0}  "B^+-Tree"\n'.format(ret[6][1]))
    f.write('27  {0}  "CUBIT"\n'.format(ret[6][2]))
    f.close()  


def run():
    gen_raw_data()

    print ('DBx1000 core')
    print ('-' * 10)
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

    print ('-' * 10)
    f = open('dat_DBx/memory.dat','w')
    for num in core_number:
        res = memory_analysis('dat_tmp_DBx/core_{}.dat'.format(num))
        print(res)
        print('\n')
        for tp in res:
            f.write('{} {} \n'.format(num, tp))
    f.close()  

    print ('-' * 10)
    # index and tuples
    run_indexAndTuples()
    
    print ('-' * 10)
    # run time
    run_latency()

def main():
    os.system('make -j')
    os.system('rm -fr graphs_DBx_*')
    itr = 0
    cmd = 'mv graphs_DBx graphs_DBx_{}'
    cmd2 = "mv dat_DBx graphs_DBx/eva_data"
    cmd3 = "mv dat_tmp_DBx graphs_DBx/eva_data"
    cmd4 = "mkdir -P graphs_DBx/eva_data"
    while itr < 3:
        os.chdir(ROOT_PATH)
        datdir = 'dat_DBx'
        if os.path.exists(datdir) and os.path.isdir(datdir):
            print ('Deleting existing directory ./dat')
            shutil.rmtree(datdir)
        os.mkdir(datdir)

        dat_tmp = 'dat_tmp_DBx'
        if os.path.exists(dat_tmp) and os.path.isdir(dat_tmp):
            print ('Deleting existing directory ./dat_tmp')
            shutil.rmtree(dat_tmp)
        os.mkdir(dat_tmp)

        graphs = 'graphs_DBx'
        if os.path.exists(graphs) and os.path.isdir(graphs):
            print ('Deleting existing directory ./graphs')
            shutil.rmtree(graphs)
        os.mkdir(graphs)

        build()
        run()
        os.system(cmd4)
        os.system(cmd3)
        os.system(cmd2)
        os.system(cmd.format(itr))
        itr += 1
    print('Done!\n')



if __name__ == '__main__':
    main()
