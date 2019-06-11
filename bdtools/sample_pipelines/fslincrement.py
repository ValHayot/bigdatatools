#!/usr/bin/env python3                                   
from boutiques.descriptor2func import function
#from time import time
import sys
import threading
import subprocess

fuse = "sharedfs"
lustre = "nonshare"
tmpfs = "/tmp/inc"
fs = None

p = subprocess.Popen("sudo sysctl vm.drop_caches=3", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
(out, err) = p.communicate()

def increment(maths, curr_in, t_id, it=30):
    for i in range(it):
        curr_out = "{0}/inc{1}-{2}.nii".format(fs, t_id, i)
        
        #start = time()
        maths_out = maths('-u', '-v -v {0}:{0}'.format(tmpfs), input_file=curr_in, dt="short", addn=1, odt="short", output_name=curr_out)
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

curr_in = "{}/bigbrain_1538_1206_2082.nii".format(fs)

threads = list()
for index in range(10):
    x = threading.Thread(target=increment, args=(maths, curr_in, index))
    threads.append(x)
    x.start()

for index, thread in enumerate(threads):
    thread.join()


