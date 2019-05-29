#!/usr/bin/env python3

from psutil import disk_partitions, disk_usage
from os import access, R_OK, W_OK, getcwd, chdir
import sys


def add_el(d, k, v):
    if k in d:
        d[k].append(v)
    else:
        d[k] = [v]


def get_mount(all_partitions, fp):
    return max([(p.mountpoint, p.fstype)
                for p in all_partitions
                if p.mountpoint in fp],
               key=lambda x: len(x[0]))


def avail_fs(working_dir=getcwd(), possible_fs=None):

    # return available mountpoints that user has rw access to
    storage = {}
    all_partitions = disk_partitions(all=True)

    for d in all_partitions:
        if access(d.mountpoint, R_OK) and access(d.mountpoint, W_OK):
            add_el(storage, d.fstype, d.mountpoint)

    # shoddy way of determining fs type of working dir
    if len([el for k,v in storage.items() for el in v if el == working_dir]) == 0:
        if access(working_dir, R_OK) and access(working_dir, W_OK):
            parent = get_mount(all_partitions, working_dir)

            add_el(storage, parent[1], working_dir)

    # some cleanup as not sure what to do with other filesystems for the moment

    if possible_fs is None:
        possible_fs = ['tmpfs', 'localdisk', 'lustre']

    if 'localdisk' in possible_fs:
        storage['localdisk'] = []

    for fs in storage.keys():
        if fs not in possible_fs:
            mounts = storage.pop(fs)

            if fs in ['ext4', 'zfs'] and 'localdisk' in storage:
                storage['localdisk'].extend(mounts)

    return storage


class HierarchicalFs:

    def __init__(self, working_dir=getcwd(),
                 possible_fs=['tmpfs', 'localdisk', 'lustre']):
        self.storage = avail_fs(working_dir, possible_fs)
        self.possible_fs = possible_fs
        # key would be basename, value is the filesystem
        #TODO: consider converting value to namedtuple where file reuse is a parameter
        # such that if file reuse = 0, lustre is selected over other filesystems
        #TODO: this file dictionary will perhaps need to be written to a file on 
        # the shared network in a distributed environment
        self.files = {}
        self.working_dir = working_dir


    def cd_to_write(self, input_files):
        # selects fs where there's at least enough space to copy all
        # input files once

        total_size = 0

        # get total size (in bytes) of files
        for f in input_files:
            total_size += op.getsize(f)

        mountpoint = self.top_fs(total_size)

        chdir(mountpoint)


    def sorted_storage(self):
        priority_fs = []

        #TODO: convert to list comprehension
        for st in self.possible_fs:
            if st in self.storage:
                priority_fs.extend(self.storage[st])

        return priority_fs
                

    def top_fs(self, size):

        priority_mounts = self.sorted_storage()
        for mount in priority_mounts:
            if disk_usage(mount).free - size > 0:
                return mount

        print("ERROR: Not enough space on any device")
        sys.exit(1)


    def track_files(self, fpaths):
        #NOTE: fpoints must be a list of absolute filepaths

        for fp in fpaths:
            fs = get_mount(self.storage, fp)[1]
            self.files[fp] = fs


    def move_files_to_wd(all_files=False):
        #TODO: if file is unlikely to be reused, move to working directory
        # if all operations are complete or all files need to be transferred to
        # wd, set all_file to True
        pass


