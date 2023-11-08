
from os import listdir, remove, chdir
import os
from os.path import isfile, join, exists
import re
import statistics as stati
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import rcParams

plt.rcParams["font.family"] = "Linux Libertine O"

def draw_cdf(data):
    sorted_data = np.sort(data)
    yvals=np.arange(len(sorted_data))/float(len(sorted_data)-1)
    plt.plot(sorted_data, yvals)
    plt.show()

def cal_percentile(data):
    for q in [50, 90, 95, 99, 99.9, 99.99, 99.999, 100]:
        print ("{}% percentile: {}".format (q, np.percentile(data, q)))
        
def check(exp_name):
    with open(exp_name) as f:
        content = f.readlines()
    content = [int(x.strip()) for x in content]
    content = sorted(content)
    print("=========", nm_map[exp_name], "=========")
    print("# items:", len(content), "    Min:", min(content), "     Max:", max(content), "    Mean:", stati.mean(content))
    cal_percentile(content)
    print("Extremely large values:")
    for i in range(-10,0):
        print(content[i])
    print("=========  END  =========")

#####################
#####################

experiments_lookup = [
                      'CUBIT',
                      'BTree',
                      'Hash',
                      'Scan'
                      ]

# file_map
file_map = {
    'Scan'   :   'dat_tmp_DBx/core_32.dat',
    'Hash'    :   'dat_tmp_DBx/core_32.dat',
    'BTree'    :   'dat_tmp_DBx/core_32.dat',
    'CUBIT'      :   'dat_tmp_DBx/core_32.dat'
}

# name_map
nm_map = {
    'Scan'   :   'SCAN ',
    'Hash'    :   'Hash ',
    'BTree'    :   'BTree ',
    'CUBIT'      :   'CUBIT '
}

# label_name_map
label_nm_map = {
    'Scan'   :   'SCAN ',
    'Hash'    :   'Hash ',
    'BTree'    :   '$\mathregular{B^+}$-Tree ',
    'CUBIT'      :   'CUBIT '
}

# linestyle_map
ls_map = {
    'Scan'   :   (0, (3, 1, 1, 1, 1, 1)),
    'Hash'   :   '-.',
    'BTree'   :   'dashed', 
    'CUBIT'        :   '-'

}

# color map
co_map = {
    'Scan'   :   'black',
    'Hash'   :   'g',
    'BTree'   :   'brown',
    'CUBIT'        :   'blue'
}


experiments = experiments_lookup

dir = os.getcwd() 
print(dir)
mydir = dir #+ '/latency/data'
chdir(mydir)

def figure():
    for exp_name in experiments:
        if exists(exp_name):
            remove(exp_name)
        f = open(file_map[exp_name])
        output = open(exp_name,'w')
        for line in f:
            a = line.split()
            if (len(a) != 3 and len(a) != 5):
                continue
            # elif (not str(a[2]).isdigit()):
            #     continue
            elif line.startswith(nm_map[exp_name]) :
                output.write(str(a[2]) + '\n')
        f.close()
        output.close()

    for exp_name in experiments:
        check(exp_name)

    fig, ax = plt.subplots()
    for file_name in experiments:
        data = np.loadtxt(file_name)
        sorted_data = np.sort(data)
        yvals=np.arange(len(sorted_data))/float(len(sorted_data)-1)
        ax.plot(sorted_data, yvals, label=label_nm_map[file_name], ls=ls_map[file_name], color=co_map[file_name], lw = '2')

    #plt.xlim(left=0,right=400000)
    plt.ylim(0, 1)
    plt.xlabel("TXN Response Time (ms)", fontsize='20')
    plt.ylabel("Cumulative Fraction", fontsize='20')
    legend = ax.legend(loc='lower right', shadow=True, fontsize='20')
    plt.tick_params(labelsize=18)
    plt.tight_layout()
    fig = 1

    if fig == 0:
        plt.show()
    else:
        plt.savefig(dir + '/graphs_DBx/cdf.png', format='png', dpi=1200)
        plt.savefig(dir + '/graphs_DBx/cdf.eps', format='eps', dpi=1200)

print("\n\n\n##################### CDF result ########################\n\n")     
figure()
for exp_name in experiments:
    if exists(exp_name):
        remove(exp_name)

 
