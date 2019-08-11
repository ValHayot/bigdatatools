#!/usr/bin/env python3

from psutil import disk_partitions, disk_usage
from os import access, R_OK, W_OK, getcwd, chdir, path as op, makedirs, listdir
from blkinfo import BlkDiskInfo
from numpy import asarray
from getpass import getuser
from socket import gethostname
from time import time
import sys


def add_el(d, k, v):
    if k in d:
        d[k].append(v)
    else:
        d[k] = [v]


def get_mount(all_partitions, fp):
    return max([(p.mountpoint, p.fstype, op.basename(p.device))
                for p in all_partitions
                if p.mountpoint in fp],
               key=lambda x: len(x[0]))


def parent_drives(dt, dn):
    parents = dt[dn]['parents']
    
    if len(parents) > 0:
        return asarray(list(map(parent_drives, 
                                [dt]*len(parents),
                                parents))).flatten()
    return dn


def set_ssd_hdd(pd, storage, disk_tree, mountpoint):
    for p in pd:
        if disk_tree[p]['rota'] == '0':
            fstype = 'ssd'
        else:
            fstype = 'hdd'
        add_el(storage, fstype, mountpoint)


def wb_contents(wblist, list_type):
    if wblist is not None:
        if not isinstance(wblist, list) and op.isfile(wblist):
            with open(wblist, 'r') as f:
                wblist = [m for m in f if op.isdir(m)]

                if len(wblist) == 0:
                    print(('ERROR: {} file does not contain any valid' +
                           'filepaths').format(list_type))
                    sys.exit(1)
        elif not isinstance(wblist, list):
            print('ERROR: {} is not a file or a list'.format(list_type))
            sys.exit(1)
    return wblist

def avail_fs(working_dir=getcwd(), possible_fs=None, whitelist=None,
             blacklist=None):

    # return available mountpoints that user has rw access to
    storage = {}
    all_partitions = disk_partitions(all=True)
    disk_tree = BlkDiskInfo()._disks

    whitelist = wb_contents(whitelist, 'whitelist') 
    blacklist = wb_contents(blacklist, 'blacklist') 

    for d in all_partitions:
        if access(d.mountpoint, R_OK) and access(d.mountpoint, W_OK):
            device = op.basename(d.device)
            mountpoint = d.mountpoint
            if (whitelist is not None and mountpoint not in whitelist):

                # check if mountpoint is one of the mounts specified in 
                # one of the whitelist's directories
                for fm in whitelist:
                    if get_mount(all_partitions, fm) == mountpoint:
                        mountpoint = fm
                        break
                continue

            # if mount is blacklisted, skip
            if blacklist is not None and mountpoint in blacklist:
                continue

            if mountpoint != working_dir:
                mountpoint = op.join(mountpoint,
                                     '{0}-{1}'.format(gethostname(), getuser()))

            if (d.fstype != 'tmpfs' and 'lustre' not in d.fstype
                and op.basename(device) in disk_tree):
                pd = parent_drives(disk_tree, device)
                set_ssd_hdd(pd, storage, disk_tree, mountpoint)
            else:
          your descriptor2func       add_el(storage, d.fstype, mountpoint)

    # shoddy way of determining fs type of working dir
    if (whitelist is None and
        len([el for k,v in storage.items()
             for el in v if el == working_dir]) == 0):

        if access(working_dir, R_OK) and access(working_dir, W_OK):
            parent = get_mount(all_partitions, working_dir)

            pd = parent_drives(disk_tree, parent[2])
            fs = parent[1]
            set_ssd_hdd(pd, storage, disk_tree, working_dir)

    # some cleanup as not sure what to do with other filesystems for the moment

    if possible_fs is None:
        possible_fs = ['tmpfs', 'ssd', 'hdd', 'lustre']

    orig_keys = [k for k in storage.keys()]
    for fs in orig_keys:
        if fs not in possible_fs:
            mounts = storage.pop(fs)

    return storage


class HierarchicalFs:

    def __init__(self, working_dir=getcwd(),
                 possible_fs=['tmpfs', 'ssd', 'hdd', 'lustre'],
                 whitelist=None, blacklist=None):
        self.storage = avail_fs(working_dir, possible_fs,
                                whitelist, blacklist)
        self.possible_fs = self.storage.keys()

        # key would be filename, value is last used timestamp 
        # (eviction policy = LRU)
        self.files = {}
        self.working_dir = working_dir


    def cd_to_write(self, input_files):
        # selects fs where there's at least enough space to copy all
        # input files once

        input_files = [op.abspath(f) for f in input_files]
        self.track_files(listdir(getcwd()))

        total_size = 0

        # get total size (in bytes) of files
        for f in input_files:
            total_size += op.getsize(f)

        mountpoint = self.top_fs(total_size)

        chdir(mountpoint)
        return mountpoint, input_files


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
            if not op.isdir(mount):
                makedirs(mount)
            if disk_usage(mount).free > size:
                return mount

        print("ERROR: Not enough space on any device")
        sys.exit(1)


    def track_files(self, fpaths):
        #NOTE: fpoints must be a list of absolute filepaths

        for fp in fpaths:
            self.files[fp] = time()
            
        for k,v in sorted(self.files.items(), key=lambda x: ):


    def move_files_to_wd(all_files=False):
        #TODO: if file is unlikely to be reused, move to working directory
        # if all operations are complete or all files need to be transferred to
        # wd, set all_file to True
        pass


