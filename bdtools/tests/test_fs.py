#!/usr/bin/env python3
import pytest
from bdtools.bdtools import HierarchicalFs
import nibabel as nib
from os import getcwd, path as op, remove
from psutil import disk_usage
from getpass import getuser
from socket import gethostname

#NOTE: tests currently assume my workstation configuration
'''
Filesystem                               Size  Used Avail Use% Mounted on
tmpfs                                    7.7G  688M  7.1G   9% /dev/shm
tmpfs                                    7.7G  520K  7.7G   1% /tmp
tmpfs                                    1.6G   34M  1.6G   3% /run/user/1000
/dev/mapper/fedora_localhost--live-home  411G  239G  151G  62% /home
'''
curr_dir = getcwd()
tmp_dir = '{0}-{1}'.format(gethostname(), getuser())
exp_sstorage = [op.join('/dev/shm', tmp_dir),
                op.join('/tmp', tmp_dir),
                op.join('/run/user/1000', tmp_dir),
                getcwd()]

def test_sorted_storage():
    ''' Test to ensure that function correctly sorted the available storage in
        terms of speed
    '''

    hfs = HierarchicalFs()
    sortstorage = hfs.sorted_storage()

    assert(sortstorage == exp_sstorage), sortstorage


def test_fit_top():
    ''' Test to determine if file fits in "top" filesystem

    '''

    hfs = HierarchicalFs()
    in_fp = op.join(curr_dir, 'tests/sample_data/img1.nii')
    mp = hfs.cd_to_write([in_fp])
    out_im = nib.load(in_fp)
    out_fn = 'output.nii'
    nib.save(out_im, out_fn)
    assert(mp==exp_sstorage[0]), mp
    expected_out = op.join(exp_sstorage[0], out_fn)
    assert(op.abspath(out_fn) == expected_out), getcwd()
    remove(expected_out)


def test_fit_second():
    ''' Test to fill up top tmpfs. verify if it can save in the second best

    '''
    hfs = HierarchicalFs()

    in_fp = op.join(curr_dir, 'tests/sample_data/img1.nii')
    out_im = nib.load(in_fp)
    out_fn = 'output-{}.nii'
    mp = hfs.cd_to_write([in_fp])
    all_outputs = []

    count = 0
    while mp == exp_sstorage[0]:
        ofn = out_fn.format(count)
        nib.save(out_im, ofn)
        all_outputs.append(op.abspath(ofn))
        count += 1
        mp = hfs.cd_to_write([in_fp])

    out_fn = out_fn.format(count)
    nib.save(out_im, out_fn)
    all_outputs.append(op.abspath(out_fn))

    # Ensure correct mountpoint was returned
    assert(mp==exp_sstorage[1]), mp

    # Verify that top fs is in fact full
    top_s = hfs.sorted_storage()[0]
    assert(disk_usage(top_s).free < op.getsize(out_fn))

    # Verify that output file was written to correct directory
    expected_out = op.join(exp_sstorage[1], out_fn)
    assert(op.abspath(out_fn) == expected_out), getcwd()

    for im in all_outputs:
        remove(im)


def test_whitelist():
    ''' Test to verify that whitelist is functioning as intended
    
    '''

    hfs = HierarchicalFs(whitelist=['/tmp'])
    ss = hfs.sorted_storage()
    assert(ss == [op.join('/tmp', tmp_dir)]), ss
    assert(list(hfs.storage.keys()) == ['tmpfs']), hfs.storage.keys() 



