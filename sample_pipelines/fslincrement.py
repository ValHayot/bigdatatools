#!/usr/bin/env python3                                   
from boutiques.descriptor2func import function
from time import time
import sys
import threading
import subprocess
from os import linesep, path as op


fuse = None
lustre = None
tmpfs = None
fs = None
maths = None
init_splits = None

def load_env(env):

    global fuse, lustre, tmpfs, maths, init_splits

    if env == "appliance":
        fuse = "/home/v_hayots/bigdatatools/sharedfs"
        lustre = "/data/vhayots/nonshare"
        tmpfs = "/dev/shm/nonshare"
        maths = function("/home/v_hayots/bigdatatools/fsl_maths.json")
        init_splits = ['bigbrain_25G.nii', 'bigbrain_0_0_1735.nii',
                       'bigbrain_0_1005_0.nii','bigbrain_0_1005_1735.nii',
                       'bigbrain_0_2010_0.nii', 'bigbrain_0_2010_1735.nii']
    elif env== "g5k":
        fuse = "/home/vhayotsasson/code/sea/fusemount"
        lustre = "/tmp/nfs/nonshared"
        tmpfs = '/dev/shm/nonshare'
        maths = function("/home/vhayotsasson/code/sea/fsl_maths.json")
        init_splits = ['bigbrain_0_0_0.nii', 'bigbrain_0_0_1388.nii',
                       'bigbrain_0_0_2082.nii','bigbrain_0_0_2776.nii',
                       'bigbrain_0_0_694.nii', 'bigbrain_0_1206_0.nii']
    else:
        fuse = "/home/valeriehayot/Documents/code/bigdatatools/sharedfs"
        init_splits = ['bigbrain_0_0_0.nii', 'bigbrain_0_0_1388.nii',
                       'bigbrain_0_0_2082.nii','bigbrain_0_0_2776.nii',
                       'bigbrain_0_0_694.nii', 'bigbrain_0_1206_0.nii']
        maths = function("/home/valeriehayot/Documents/code/bigdatatools/fsl_maths.json")

p = subprocess.Popen("sudo sysctl vm.drop_caches=3", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
(out, err) = p.communicate()

start = time()

def increment(maths, curr_in, t_id, it=5):
    for i in range(it):
        curr_out = "{0}/inc{1}-{2}.nii".format(fs, t_id, i)
        #cmd = "vmtouch {0} >> logs/{1}-t{2}.log".format(curr_in, fs, t_id)
        #p = subprocess.Popen(cmd, shell=True)
        #start = time()
        #maths_out = maths('-u', '-v{0}:{0}'.format(tmpfs), '-v{0}:{0}'.format(lustre), input_file=curr_in, dt="short", addn=1, odt="short", output_name=curr_out)
        maths_out = maths('-u', input_file=curr_in, dt="short", addn=1, odt="short", output_name=curr_out)
        print(maths_out)
        #print(time()-start)
        curr_in = curr_out

load_env(sys.argv[5])

if sys.argv[1] == "fuse":
    fs = fuse
elif sys.argv[1] == "tmpfs":
    fs = tmpfs
else:
    fs = lustre

num_its = int(sys.argv[2])
num_threads = int(sys.argv[3])
block_size = sys.argv[4]


threads = list()
for index in range(num_threads):
    curr_in = op.join(fs, init_splits[index])
    x = threading.Thread(target=increment, args=(maths, curr_in, index, num_its))
    threads.append(x)
    x.start()

for index, thread in enumerate(threads):
    thread.join()

of = 'fslinc_{}_t{}_i{}_bs{}.out'.format(sys.argv[1], num_threads, num_its, block_size)

with open(of, 'a+') as f:
    f.write(str(time()-start) + linesep)

print("Total execution time:", time() - start)
