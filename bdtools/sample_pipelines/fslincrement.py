#!/usr/bin/env python3                                   
from boutiques.descriptor2func import function
import sys


fuse = "sharefs"
lustre = "nonshare"
tmpfs = "/tmp/inc"
fs = None

if sys.argv[1] == "fuse":
    fs = fuse
elif sys.argv[1] == "tmpfs":
    fs = tmpfs
else:
    fs = lustre

maths = function("fsl_maths.json")
curr_in = "{}/bigbrain_1540_1815_2100.nii.gz".format(fs)
for i in range(100):
    curr_out = "{0}/inc{1}.nii".format(fs, i)
    maths_out = maths('-u', input_file=curr_in, dt="short", addn=1, odt="short", output_name=curr_out)
    print(maths_out)
    curr_in = curr_out
