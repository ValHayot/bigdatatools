#!/usr/bin/env python3                                   
from boutiques.descriptor2func import function
from time import time
import sys
import threading
import subprocess
from os import path as op


fuse = "sharedfs"
lustre = "/data/vhayots/nonshare"
tmpfs = "/tmp/inc"
fs = None

p = subprocess.Popen("sudo sysctl vm.drop_caches=3", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
(out, err) = p.communicate()

start = time()
def increment(maths, curr_in, t_id, it=5):
    for i in range(it):
        curr_out = "{0}/inc{1}-{2}.nii".format(fs, t_id, i)
        #cmd = "vmtouch {0} >> logs/{1}-t{2}.log".format(curr_in, fs, t_id)
        #p = subprocess.Popen(cmd, shell=True)
        #start = time()
        maths_out = maths('-u', '-v{0}:{0}'.format(tmpfs), '-v{0}:{0}'.format(lustre), input_file=curr_in, dt="short", addn=1, odt="short", output_name=curr_out)
        print(maths_out)
        #print(time()-start)
        curr_in = curr_out

if sys.argv[1] == "fuse":
    fs = fuse
elif sys.argv[1] == "tmpfs":
    fs = tmpfs
else:
    fs = lustre

maths = function("fsl_maths.json")

init_splits = ['bigbrain_0_0_0.nii', 'bigbrain_0_0_1735.nii',
               'bigbrain_0_1005_0.nii','bigbrain_0_1005_1735.nii',
               'bigbrain_0_2010_0.nii', 'bigbrain_0_2010_1735.nii']

threads = list()
for index in range(2):
    
    curr_in = op.join(fs, init_splits[index])
    x = threading.Thread(target=increment, args=(maths, curr_in, index))
    threads.append(x)
    x.start()

for index, thread in enumerate(threads):
    thread.join()

print("Total execution time:", time() - start)


