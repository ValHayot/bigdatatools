#!/usr/bin/env python3

import sys
import nibabel as nib
import numpy as np
import itertools as it



def get_multiples(n):
    # check all numbers that divide n
    sqr_root = int(n ** 1/2)

    multiples = [i for i in range(1, sqr_root) if n % i == 0]
    
    all_perms = it.product(*[multiples, multiples, multiples])

    for i,j,k in all_perms:
        if i*j*k == n and i < 32767 and j < 32767 and k < 32767:
            print(i, j, k)
            return sorted([i, j, k], reverse=True)

    return (n, 1, 1) 


f_size = int(sys.argv[1]) * int(sys.argv[2]) * 1024 # input in kibibytes

print('Creating an image of size:', f_size)

f_size -= 352 # ignore nifti header size

data_type = np.uint8 # 1 byte per voxel

data = np.random.randint(0, 255, size=get_multiples(f_size), dtype=data_type)
im = nib.Nifti1Image(data, np.eye(4))

nib.save(im, 'bench_in.nii')






