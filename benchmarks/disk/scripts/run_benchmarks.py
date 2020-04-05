#!/usr/bin/env python3

import random
import sys
import subprocess

script = sys.argv[1]
iterations = 10

devices = ['/dev/shm/benchfile.txt', '/tmp/benchfile.txt', '/mnt/lustre/vhs/benchfile']
rand_device = devices*10
random.shuffle(rand_device)

for dev in rand_device:
    p = subprocess.Popen([script, dev], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (out, err) = p.communicate()
    print(str(out))

