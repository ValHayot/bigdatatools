#!/usr/bin/env python3
import pytest
from bdtools import hfs as sea
import nibabel as nib
from os import getcwd, path as op, remove
from psutil import disk_usage
from getpass import getuser
from socket import gethostname

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


def flatten_dict(l):
    return [path for fs_list in l.values() for path in fs_list]


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
