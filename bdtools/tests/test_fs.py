#!/usr/bin/env python3
import pytest
from bdtools.bdtools import HierarchicalFs
import nibabel as nib
from os import getcwd, path as op, remove

#NOTE: tests currently assume my workstation configuration
'''
Filesystem                               Size  Used Avail Use% Mounted on
tmpfs                                    7.7G  688M  7.1G   9% /dev/shm
tmpfs                                    7.7G  520K  7.7G   1% /tmp
tmpfs                                    1.6G   34M  1.6G   3% /run/user/1000
/dev/mapper/fedora_localhost--live-home  411G  239G  151G  62% /home
'''
curr_dir = getcwd()

def test_fit_tmpfs():
    ''' Test to determine if file fits in "top" filesystem

    '''

    hfs = HierarchicalFs()
    in_fp = op.join( curr_dir, 'tests/sample_data/img1.nii')
    mp = hfs.cd_to_write([in_fp])
    out_im = nib.load(in_fp)
    out_fn = 'output.nii'
    nib.save(out_im, out_fn)
    assert(mp=='/dev/shm'), mp
    expected_out = op.join('/dev/shm', out_fn)
    assert(op.abspath(out_fn) == expected_out), getcwd()
    remove(expected_out)

def test_tmpfs2():
    ''' Test to fill up top tmpfs. verify if it can save in the second best

    '''
    hfs = HierarchicalFs()

    in_fp = op.join(curr_dir, 'tests/sample_data/img1.nii')
    out_im = nib.load(in_fp)
    out_fn = 'output-{}.nii'
    mp = hfs.cd_to_write([in_fp])

    count = 0
    while mp == '/dev/shm':
        nib.save(out_im, out_fn.format(count))
        count += 1
        mp = hfs.cd_to_write([in_fp])

    out_fn = out_fn.format(count)
    nib.save(out_im, out_fn)
    assert(mp=='/tmp'), mp
    expected_out = op.join('/tmp', out_fn)
    assert(op.abspath(out_fn) == expected_out), getcwd()

