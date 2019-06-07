#!/usr/bin/env python3

from __future__ import with_statement

import os
import sys
import errno
import stat
from psutil import disk_partitions, disk_usage
from blkinfo import BlkDiskInfo
from numpy import asarray
from getpass import getuser
from socket import gethostname
from time import time
from shutil import move

from refuse.high import FUSE, FuseOSError, Operations


# global helpers
def add_el(d, k, v):
    if k in d:
        d[k].append(v)
    else:
        d[k] = [v]


def get_mount(all_partitions, fp):
    return max([(p.mountpoint, p.fstype, os.path.basename(p.device))
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
        if not isinstance(wblist, list) and os.path.isfile(wblist):
            with open(wblist, 'r') as f:
                wblist = [m for m in f if os.path.isdir(m)]

                if len(wblist) == 0:
                    print(('ERROR: {} file does not contain any valid' +
                           'filepaths').format(list_type))
                    sys.exit(1)
        elif not isinstance(wblist, list):
            print('ERROR: {} is not a file or a list'.format(list_type))
            sys.exit(1)
    return wblist


def avail_fs(working_dir=os.getcwd(), possible_fs=None, whitelist=None,
             blacklist=None):

    # return available mountpoints that user has rw access to
    storage = {}
    all_partitions = disk_partitions(all=True)
    disk_tree = BlkDiskInfo()._disks

    whitelist = wb_contents(whitelist, 'whitelist') 
    blacklist = wb_contents(blacklist, 'blacklist') 

    for d in all_partitions:
        if os.access(d.mountpoint, os.R_OK) and os.access(d.mountpoint, os.W_OK):
            device = os.path.basename(d.device)
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
                mountpoint = os.path.join(mountpoint,
                                     '{0}-{1}'.format(gethostname(), getuser()))

            if (d.fstype != 'tmpfs' and 'lustre' not in d.fstype
                and os.path.basename(device) in disk_tree):
                pd = parent_drives(disk_tree, device)
                set_ssd_hdd(pd, storage, disk_tree, mountpoint)
            else:
                add_el(storage, d.fstype, mountpoint)

    # shoddy way of determining fs type of working dir
    if (whitelist is None and
        len([el for k,v in storage.items()
             for el in v if el == working_dir]) == 0):

        if os.access(working_dir, os.R_OK) and os.access(working_dir, os.W_OK):
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


# Adapted from: https://www.stavros.io/posts/python-fuse-filesystem/

class HierarchicalFs(Operations):
    def __init__(self, working_dir):
        self.storage = avail_fs(working_dir=working_dir) 
        self.possible_fs = self.storage.keys()
        self.hierarchy = self.sorted_storage()


    # Helpers
    # =======

    # modified
    def _full_path(self, partial):
        partial = partial.lstrip("/")

        exists, path = self.path_exists(partial)
        if not exists:
            path = os.path.join(self.top_fs(), partial)
        return path

    # modified
    def path_exists(self, partial):

        for storage in self.hierarchy:
            fp = os.path.join(storage, partial)
            if os.path.exists(fp):
                return True, fp
        return False, None


    def sorted_storage(self):
        priority_fs = []

        #TODO: convert to list comprehension
        for st in self.possible_fs:
            if st in self.storage:
                priority_fs.extend(self.storage[st])

        return priority_fs
                

    def top_fs(self, size=1.049*(10**6)):

        for mount in self.hierarchy:
            if not os.path.isdir(mount):
                os.makedirs(mount)
            # must be at least 1MiB of space
            if disk_usage(mount).free > 1.049*(10**6):
                print(mount, disk_usage(mount).free)
                return mount

        print("ERROR: Not enough space on any device")
        #sys.exit(1)

    # Filesystem methods
    # ==================

    def access(self, path, mode):
        full_path = self._full_path(path)
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        full_path = self._full_path(path)
        return os.chmod(full_path, mode)

    def chown(self, path, uid, gid):
        full_path = self._full_path(path)
        return os.chown(full_path, uid, gid)

    # modified
    def getattr(self, path, fh=None):
        full_path = self._full_path(path)

        if os.path.exists(full_path):
            st = os.lstat(full_path)
        else:
            st = os.lstat(full_path)
            # TODO: perhaps handle pipeline execution here
            print("Error: {} does not exist.".format(full_path))
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                     'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size',
                     'st_uid', 'st_blocks'))

    # modified
    def readdir(self, path, fh):

        dirents = ['.', '..']

        all_paths = [os.path.join(s, path.lstrip("/")) for s in self.hierarchy]

        for full_path in all_paths:
            if os.path.isdir(full_path):
                dirents.extend(os.listdir(full_path))
        for r in dirents:
            yield r

    # modified
    def readlink(self, path):
        pathname = os.readlink(self._full_path(path))
        if pathname.startswith("/"):
            # Path name is absolute, sanitize it.
            if len(self.root.split(':')) > 1:
                return os.path.relpath(pathname, self.root.split(':')[1])
            else:
                return os.path.relpath(pathname, self.root)
        else:
            return pathname

    def mknod(self, path, mode, dev):
        return os.mknod(self._full_path(path), mode, dev)

    def rmdir(self, path):
        full_path = self._full_path(path)
        return os.rmdir(full_path)

    def mkdir(self, path, mode):
        return os.mkdir(self._full_path(path), mode)

    def statfs(self, path):
        full_path = self._full_path(path)
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def unlink(self, path):
        return os.unlink(self._full_path(path))

    def symlink(self, name, target):
        return os.symlink(name, self._full_path(target))

    def rename(self, old, new):
        return os.rename(self._full_path(old), self._full_path(new))

    def link(self, target, name):
        return os.link(self._full_path(target), self._full_path(name))

    def utimens(self, path, times=None):
        return os.utime(self._full_path(path), times)

    # File methods
    # ============

    def open(self, path, flags):
        full_path = self._full_path(path)
        return os.open(full_path, flags)

    def create(self, path, mode, fi=None):
        full_path = self._full_path(path)
        return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

    def read(self, path, length, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)

    # modified
    def write(self, path, buf, offset, fh):
        out = None
        failed = True
        fp = self._full_path(path)

        try:
            os.lseek(fh, offset, os.SEEK_SET)
            out = os.write(fh, buf)
        except OSError as e:
            
            print('ERROR: ', str(e))
            # may remove
            if 'file descriptor' in str(e):
                sys.exit(1)

            next_mount = self.top_fs(os.path.getsize(fp))
            new_fp = os.path.join(next_mount, os.path.basename(fp))

            if fp != new_fp:
                #TODO: test what happens when there are consecutive failures
                print('Out of memory: ', fp, '--->', new_fp)
                move(fp, new_fp)
                new_fh = os.open(new_fp, os.O_CREAT | os.O_RDWR)
                os.dup2(new_fh, fh)
                fp = new_fp

        return out

    
    def truncate(self, path, length, fh=None):
        full_path = self._full_path(path)
        with open(full_path, 'r+') as f:
            f.truncate(length)

    def flush(self, path, fh):
        return os.fsync(fh)

    def release(self, path, fh):
        os.chmod(fh, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        return os.close(fh)

    def fsync(self, path, fdatasync, fh):
        return self.flush(path, fh)


def main(mountpoint, wd):
    
    FUSE(HierarchicalFs(os.path.abspath(wd)), mountpoint, nothreads=True, foreground=True)

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
