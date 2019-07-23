#!/usr/bin/env python3
import pytest
from bdtools import hfs as sea
import nibabel as nib
import numpy as np
import shutil
import hashlib
from os import getcwd, path as op, remove
from psutil import disk_usage
from getpass import getuser
from socket import gethostname
import subprocess

# NOTE: tests currently assume my workstation configuration
"""
Filesystem                               Size  Used Avail Use% Mounted on
tmpfs                                    7.7G  688M  7.1G   9% /dev/shm
tmpfs                                    7.7G  520K  7.7G   1% /tmp
tmpfs                                    1.6G   34M  1.6G   3% /run/user/1000
/dev/mapper/fedora_localhost--live-home  411G  239G  151G  62% /home
"""
curr_dir = getcwd()
tmp_dir = "{0}-{1}".format(gethostname(), getuser())
exp_sstorage = [
    op.join("/dev/shm", tmp_dir),
    op.join("/tmp", tmp_dir),
    op.join("/run/user/1000", tmp_dir),
    getcwd(),
]

shared = op.join(curr_dir, "sharedfs")
work = op.join(curr_dir, "workdir")
sea_cmd = [
    "python",
    op.join(curr_dir, "bdtools/hfs.py"),
    shared,
    "-o",
    "root={}".format(work),
    "-o",
    "big_writes",
    "-o",
    "log=DEBUG",
    "-o",
    "auto_unmount",
]
test_file = "bigbrain_1538_1206_2082.nii"


# Start FS
p = subprocess.Popen(sea_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
out, err = p.communicate()


def flatten_dict(l):
    return [path for fs_list in l.values() for path in fs_list]


def get_f_md5(fn):
    with open(fn, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()


def test_available_fs():
    """ Test to ensure that available storage is appropriately detected

    """
    avail_fs = sea.avail_fs()

    assert ["tmpfs", "ssd"] == list(avail_fs.keys())

    for fs in flatten_dict(avail_fs):
        assert fs in exp_sstorage

    # test limiting the possible filesystems
    avail_fs = sea.avail_fs(possible_fs=["ssd"])
    flattened_fs = flatten_dict(avail_fs)
    assert (
        list(avail_fs.keys()) == ["ssd"]
        and len(flattened_fs) == 1
        and flattened_fs[0] == exp_sstorage[3]
    ), avail_fs

    # test providing a whitelist as a list
    whitelist = ["/tmp", exp_sstorage[3]]
    avail_fs = sea.avail_fs(whitelist=whitelist)
    flattened_fs = flatten_dict(avail_fs)

    assert list(avail_fs.keys()) == ["tmpfs", "ssd"] and flattened_fs == [
        exp_sstorage[1],
        exp_sstorage[3],
    ]

    # test providing a blacklist as a list
    blacklist = ["/tmp", "/run/user/1000"]

    avail_fs = sea.avail_fs(blacklist=blacklist)
    flattened_fs = flatten_dict(avail_fs)

    assert list(avail_fs.keys()) == ["tmpfs", "ssd"] and flattened_fs == [
        exp_sstorage[0],
        exp_sstorage[3],
    ], avail_fs

    # TODO: test providing whitelist as a file


def test_write_file():
    out_file = op.join(shared, "testwrite.out")
    file_txt = "Hello World!"

    with open(out_file, "w") as f:
        f.write(file_txt)

    assert op.isfile(out_file)

    with open(out_file, "r") as f:
        output = f.read()
        assert output == file_txt, output

    remove(out_file)


def test_read_file():
    im_fuse = nib.load(op.join(shared, test_file))
    im_work = nib.load(op.join(work, test_file))

    data_fuse = im_fuse.get_data()
    data_work = im_fuse.get_data()

    header_fuse = str(data_fuse)
    header_work = str(data_work)
    assert np.array_equal(data_fuse, data_work) and header_fuse == header_work


def test_copy_file():
    im = "img1.nii"
    sample_file = op.join(curr_dir, "bdtools/tests/sample_data", im)
    fuse_file = op.join(curr_dir, shared, im)
    shutil.copy2(sample_file, fuse_file)

    orig_md5 = get_f_md5(sample_file)
    fuse_md5 = get_f_md5(fuse_file)

    remove(fuse_file)


def test_create_dir():
    pass


def test_write_to_dir():
    pass


def test_remove_dir():
    pass
