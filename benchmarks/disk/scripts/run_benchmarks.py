#!/usr/bin/env python3

import random
import sys
import subprocess

script = sys.argv[1]
benchmark_file = sys.argv[2]
iterations = sys.argv[3]

devices = ['/dev/shm/benchfile.txt', '/tmp/benchfile.txt', '/mnt/lustre/vhs/benchfile.txt']
rand_device = devices*10
random.shuffle(rand_device)

with open(benchmark_file, 'w+') as f:
    f.write('device,mode,bandwidth,unit\n')

for dev in rand_device:
    p = subprocess.Popen([script, dev, benchmark_file], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (out, err) = p.communicate()

