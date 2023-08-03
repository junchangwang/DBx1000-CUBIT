from os import listdir, remove, chdir
import os
from os.path import isfile, join, exists
import re
import statistics as stati
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import  gridspec
import sys

dir = os.getcwd()

plt.rcParams['font.family'] = ['Linux Libertine O']

# x
x_val = [1, 4, 8, 16, 24, 32]

# y
Scan = []
Hash = []
B_plus_Tree = []
BwTree = []
Trie = []
CUBIT = []

Bar_val = []

algorithm_lookup_1 = ['CUBIT',
                      'BwTree',
                      'Trie',
                      'Hash',
                      'Scan'
                      ]
                      
algorithm_lookup_2 = ['Hash',
                      'BTree',
                      'Trie',
                      'CUBIT'
                      ]
                      

list_map = {
    'Scan'   :   Scan,
    'Hash'   :   Hash,
    'BTree'  :   B_plus_Tree,
    'CUBIT'  :   CUBIT,
    'Trie'   :   Trie,
    'BwTree' :   BwTree
}

label_nm_map = {
    'Scan'   :   'SCAN',
    'Hash'   :   'Hash',
    'BTree'  :   '$\mathregular{B^+}$-Tree',
    'CUBIT'  :   'Our approach',
    'Trie'   :   'Trie',
    'BwTree' :   '$\mathregular{B^+}$-Tree'
}

ls_map = {
    'Scan'   :   (0, (3, 1, 1, 1, 1, 1)),
    'Hash'   :   '-.',
    'BTree'  :   'dashed', 
    'CUBIT'  :   '-',
    'BwTree' :   '--',
    'Trie'   :   ':'
}

co_map = {
    'Scan'   :   'black',
    'Hash'   :   'darkgreen',
    'BTree'  :   'darkred',
    'CUBIT'  :   'blue',
    'BwTree' :   'darkred',
    'Trie'   :   'purple'
}

mark_map = {
    'Scan'   :   'D',
    'Hash'   :   'v',
    'BTree'  :   '^',
    'CUBIT'  :   'o',
    'BwTree' :   '^',
    'Trie'   :   'x'
}


def get_data():
    f = open('dat_DBx/core.dat')
    pos = 0  
    for line in f:
        a = line.split()
        if(pos % 6 == 0): 
            Scan.append(float(a[1]))
        elif(pos % 6 == 1): 
            Hash.append(float(a[1]))
        elif(pos % 6 == 2): 
            B_plus_Tree.append(float(a[1]))
        elif(pos % 6 == 3): 
            BwTree.append(float(a[1]))
        elif(pos % 6 == 4): 
            Trie.append(float(a[1]))
        elif(pos % 6 == 5): 
            CUBIT.append(float(a[1]))
        pos += 1
    f.close()
    
    f = open('dat_DBx/memory.dat')
    for line in f:
        a = line.split()
        Bar_val.append(float(a[1]) / 1024)
    f.close()


def draw():
    #plt.rcParams['font.size'] = 20
    plt.figure(figsize = (6, 6.8))

    gs = gridspec.GridSpec(2, 1, height_ratios = [1.8, 1], hspace = 0.4)
    ax1 = plt.subplot(gs[0, 0])
    ax2 = plt.subplot(gs[1, 0])
    ax1.tick_params(labelsize = 22)
    ax2.tick_params(labelsize = 22)
    
    #### core plot
    for exp in algorithm_lookup_1:
        ax1.plot(x_val, list_map[exp], marker = mark_map[exp], ms = 11, mfc = 'w', c = co_map[exp], ls = ls_map[exp], lw = 2, label = label_nm_map[exp])
        
    
    ax1.set_ylabel("Throughput (queries/s)", fontsize= 21)
    ax1.set_xlabel("Number of cores", fontsize= 22)
    ax1.set_xticks(x_val)
    ax1.legend(loc = 'upper left', fontsize = 16, frameon = False)
    
    #### memory bar
    ax2.set_ylim(0, 2.4)
    ax2.set_yticks([0, 0.5, 1, 1.5, 2])
    
    ax2.bar('Hash', Bar_val[0], lw = 0.8, fc = 'darkgreen', width = 0.5) 
    ax2.bar('$\mathregular{B^+}$-Tree ', Bar_val[1], lw = 0.8, fc = 'darkred', width = 0.5)
    ax2.bar('Trie', Bar_val[3], lw = 0.8, fc = 'purple', width = 0.5)
    ax2.bar('Ours', Bar_val[4], lw = 0.8, fc = 'blue', width = 0.5)
    
    ## text
    ax2.text(0, Bar_val[0] + 0.05, round(Bar_val[0], 2), ha = 'center', va = "bottom", fontsize = 20)
    ax2.text(1, Bar_val[1] + 0.05, round(Bar_val[1], 2), ha = 'center', va = "bottom", fontsize = 20)
    ax2.text(2, Bar_val[3] + 0.05, round(Bar_val[3], 2), ha = 'center', va = "bottom", fontsize = 20)
    ax2.text(3, Bar_val[4] + 0.05, round(Bar_val[4], 2), ha = 'center', va = "bottom", fontsize = 20)
    
    ax2.set_ylabel("Memory Size (GB)", fontsize = 22)
    ax2.set_xlabel("(b)", fontsize = 20)
              
    #plt.show()
    
    plt.savefig('Motivation.eps', format='eps', bbox_inches = 'tight', dpi= 1200, pad_inches = 0.02)
    
def main():
    if len(sys.argv) < 2:
        print ("Must specify the directory containing experimental results.")
    else:
        print ("[motivation.py] To process director ", sys.argv[1])
 
    if os.path.exists(sys.argv[1] + '/eva_data'):
        os.chdir(sys.argv[1] + "/eva_data")      
    get_data()
    
    os.chdir("../graphs_DBx")
    draw()
    
    print('[motivation] Done!\n')

if __name__ == '__main__':
    main()

