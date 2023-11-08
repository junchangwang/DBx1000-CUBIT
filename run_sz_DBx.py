
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
def build():      
    os.system("./rundb -M build")


def gen_raw_data(): 
    for num in core_number:
        cmd = './rundb -t{} -M cache > dat_tmp_DBx/core_{}.dat'.format(num, num)
        os.system(cmd)
        print ('Data file dat_tmp_DBx/core_{}.dat has been generated.'.format(num))

def main():
    os.system('make -j')
    os.system('rm -fr graphs_DBx_*')
    itr = 0
    cmd = 'mv graphs_DBx graphs_DBx_{}'
    cmd2 = "mv dat_DBx graphs_DBx/eva_data"
    cmd3 = "mv dat_tmp_DBx graphs_DBx/eva_data"
    cmd4 = "mkdir -p graphs_DBx/eva_data"
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
        gen_raw_data()
        os.system(cmd4)
        os.system(cmd3)
        # os.system(cmd2)
        os.system(cmd.format(itr))
        itr += 1
    print('Done!\n')

if __name__ == '__main__':
    main()
