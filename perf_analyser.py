#!/usr/bin/python3

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

# Data set
cache_references = []
cache_misses = []
cycles = []
instructions = []
branches = []
branch_misses = []
page_faults = []
cpu_migrations = []
###
L1_dcache_loads = []
L1_dcache_load_misses = []
L1_icache_load_misses = []
LLC_loads = []
LLC_load_misses = []
dTLB_loads = []
dTLB_load_misses = []
####
seconds = []

def process_output(filename):
    f = open(filename)

    for line in f:
        line = line.replace(',','')
        line = line.replace('<not counted>','0')
        a = line.split()
        if (len(a) < 2):
            continue
        if a[1] == 'cache-references':
            cache_references.append(float(a[0]))
        if a[1] == 'cache-misses':
            cache_misses.append(float(a[0]))
        if a[1] == 'cycles':
            cycles.append(float(a[0]))
        if a[1] == 'instructions':
            instructions.append(float(a[0]))
        if a[1] == 'branches':
            branches.append(float(a[0]))
        if a[1] == 'branch-misses':
            branch_misses.append(float(a[0]))
        if a[1] == 'page-faults':
            page_faults.append(float(a[0]))
        if a[1] == 'cpu-migrations':
            cpu_migrations.append(float(a[0]))

        if a[1] == 'L1-dcache-loads':
            L1_dcache_loads.append(float(a[0]))
        if a[1] == 'L1-dcache-load-misses':
            L1_dcache_load_misses.append(float(a[0]))
        if a[1] == 'L1-icache-load-misses':
            L1_icache_load_misses.append(float(a[0]))
        if a[1] == 'LLC-loads':
            LLC_loads.append(float(a[0]))
        if a[1] == 'LLC-load-misses':
            LLC_load_misses.append(float(a[0]))
        if a[1] == 'dTLB-loads':
            dTLB_loads.append(float(a[0]))
        if a[1] == 'dTLB-load-misses':
            dTLB_load_misses.append(float(a[0]))                                                            

        if a[1] == 'seconds':
            seconds.append(float(a[0]))
            
def printout(name, tmp_list):
    print(name + ': \n\t' + str(round(sum(tmp_list))) + "\n\t" + 
            str(len(tmp_list)) + "\n\t" + str(round(sum(tmp_list)/len(tmp_list))))

def missrate(name, misses, accesses):
    print('--- ' + name + ' miss rate: \t' + str("{:.2f}".format(sum(misses) / sum(accesses)*100)) + '%')

def analyzer():
    printout('cache-references', cache_references)
        
    printout('cache-misses', cache_misses)

    ###
    missrate('Cache', cache_misses, cache_references)

    printout('cycles', cycles)

    printout('instructions', instructions)

    print('--- CPI: \t' + str(sum(cycles) / sum(instructions)))

    printout('branches', branches)
    
    printout('branch-misses', branch_misses)

    ###
    missrate('Branch', branch_misses, branches)

    printout('page-faults', page_faults)

    printout('cpu-migrations', cpu_migrations)

    printout('L1-dcache-loads', L1_dcache_loads)

    printout('L1-dcache-loads-misses', L1_dcache_load_misses)

    ###
    missrate('L1', L1_dcache_load_misses, L1_dcache_loads)

    printout('LLC-loads', LLC_loads)

    printout('LLC-loads-misses', LLC_load_misses)

    ###
    missrate('LLC', LLC_load_misses, LLC_loads)

    printout('dTLB-loads', dTLB_loads)

    printout('dTLB-load-misses', dTLB_load_misses)

    ###
    missrate('TLB', dTLB_load_misses, dTLB_loads)

    printout('seconds', seconds)


def main():
    # ret = analyser('perf.output.CUBIT.121774')
    print ("[perf_analyser]: Start analyzing output file: " + str(sys.argv[1]))
    
    process_output(str(sys.argv[1]))
    analyzer()
    
    print ('[perf_analyser]: Done!\n')


if __name__ == '__main__': 
    main()
