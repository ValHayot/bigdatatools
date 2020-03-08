import nibabel as nib
import numpy as np
from time import sleep, time_ns as time
from os import path as op, linesep
from io import BytesIO
import sys


def increment(infile, outdir, fs, delay, start, bench):

    s_r = time()
    with open(infile, 'rb') as f:
        fh = nib.FileHolder(fileobj=BytesIO(f.read()))
        img = nib.Nifti1Image.from_file_map({"header": fh, "image": fh})
        data = np.asanyarray(img.dataobj)
    e_r = time()

    s_i = time()
    data = data + 1
    sleep(delay)
    img_inc = nib.Nifti1Image(data, img.affine, img.header)
    e_i = time()

    s_w = time()
    nib.save(img_inc, op.join(outdir, op.basename(infile)))
    e_w = time()

    bench.write(",".join([fs, infile, str(delay), str(start), str(s_r), str(e_r), str(s_i), str(e_i), str(s_w), str(e_w) + linesep]))

    return infile

if __name__=="__main__":

    infile = sys.argv[1]
    outdir = sys.argv[2]
    fs = sys.argv[3]
    it = int(sys.argv[4])
    delay = int(sys.argv[5])

    start = time()
    with open("./increment/{0}_{1}_{2}-benchmarks.out".format(fs, it, op.basename(infile)), "w+") as bench:

        bench.write('fs,fn,delay,start,read_s,read_e,increment_s,increment_e,write_s,write_e' + linesep)
        for i in range(it):
            infile = increment(infile, outdir, fs, delay, start, bench)



