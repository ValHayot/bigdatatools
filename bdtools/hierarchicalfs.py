#!/usr/bin/env python3

from __future__ import with_statement

import os
import sys
import errno
import stat
import atexit
import signal
import subprocess
import logging
import argparse
#import asyncio remove as was not working. will investigate further
from threading import Thread
from psutil import disk_partitions, disk_usage
from blkinfo import BlkDiskInfo
from numpy import asarray
from getpass import getuser
from socket import gethostname
from time import time, sleep
from shutil import move

from refuse.high import FUSE, FuseOSError, Operations


# global helpers
def add_el(d, k, v):
    if k in d and v not in d[k]:
        d[k].append(v)
    else:
        d[k] = [v]


def get_mount(all_partitions, fp):
    return max([(p.mountpoint, p.fstype, os.path.basename(p.device) if p.device.startswith("/") else p.device)
                for p in all_partitions
                if p.mountpoint in fp],
               key=lambda x: len(x[0]))


def parent_drives(dt, dn):
    parents = []
    if dn in dt:
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
                wblist = [m.strip(os.linesep).rstrip("/") for m in f if os.path.isdir(m.strip(os.linesep))]
                #print(wblist)

                if len(wblist) == 0:
                    #print(('ERROR: {} file does not contain any valid ' +
                    #       'filepaths').format(list_type))
                    sys.exit(1)
        elif not isinstance(wblist, list):
            #print('ERROR: {} is not a file or a list'.format(list_type))
            sys.exit(1)
    return wblist


def avail_fs(working_dir=os.getcwd(), possible_fs=None, whitelist=None,
             blacklist=None):

    # return available mountpoints that user has rw access to
    storage = {}
    all_partitions = disk_partitions(all=True)
    disk_tree = BlkDiskInfo()._disks
    #print("\n\n".join(["{0} : {1}".format(k,v) for k,v in disk_tree.items() if 'lustrefs' in str(v)]))
    #print("\n\n".join([str(el) for el in all_partitions if 'lustrefs' in str(el)]))

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
    if (len([el for k,v in storage.items()
             for el in v if el == working_dir]) == 0):

        if os.access(working_dir, os.R_OK) and os.access(working_dir, os.W_OK):
            parent = get_mount(all_partitions, working_dir)

            pd = parent_drives(disk_tree, parent[2])
            fs = parent[1]
            if fs != 'lustre':
                set_ssd_hdd(pd, storage, disk_tree, working_dir)
            else:
                add_el(storage, fs, working_dir)

    # some cleanup as not sure what to do with other filesystems for the moment

    if possible_fs is None:
        possible_fs = ['tmpfs', 'ssd', 'hdd', 'lustre']

    orig_keys = [k for k in storage.keys()]
    for fs in orig_keys:
        if fs not in possible_fs:
            mounts = storage.pop(fs)
            
    # ensure all temp working dirs are created
    for k,v in storage.items():
        for mount in v:
            if not os.path.isdir(mount):
                os.makedirs(mount)

    return storage

def mv2workdir(hierarchy, working_dir, delay=10):
    #TODO: improve policy
    # remove file with oldest last access time
    while True:
        sleep(delay)
        file_access = {}
        for mount in hierarchy:
            if mount != working_dir:
                for dirpath, _, files in os.walk(mount):
                    for f in files:
                        fp = os.path.join(dirpath, f)
                        
                        # only cp if file is readonly
                        if not os.access(fp, os.W_OK):
                            file_access[os.path.getatime(fp)] = (fp, os.path.relpath(dirpath, mount))

        if len(file_access.keys()) > 1:
            fp, subdir = file_access[sorted(file_access.keys())[0]]
            out_dir = os.path.join(working_dir, subdir) 
            #print('Moving file', fp, '-->', out_dir)
            move(fp, out_dir)


# Adapted from: https://www.stavros.io/posts/python-fuse-filesystem/

class HierarchicalFs(Operations):
    def __init__(self, working_dir, log, whitelist=None, blacklist=None):
        
        numeric_lvl = getattr(logging, log.upper())

        logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                            datefmt='%d/%m/%Y %I:%M:%S %p',
                            level=numeric_lvl)
        logging.info("Setting up storage")
        self.storage = avail_fs(working_dir=working_dir, whitelist=whitelist) 
        self.possible_fs = self.storage.keys()
        self.hierarchy = self.sorted_storage()

        #print("\n".join("{0}: {1}".format(k, v) for k,v in self.storage.items()))

        logging.debug("Storage hierarchy: {}".format(" -> ".join(self.hierarchy)))

        self.working_dir = working_dir

        atexit.register(self.cleanup)
        signal.signal(signal.SIGTERM, self.cleanup)
        signal.signal(signal.SIGINT, self.cleanup)

        logging.debug("Starting up async thread to flush")
        self.thread = Thread(target=mv2workdir, args=(self.hierarchy, self.working_dir))
        self.thread.setDaemon(True)
        self.thread.start()
        #self.loop = asyncio.get_event_loop()
        #self.mv2workdir()


    # Helpers
    # =======


        #return self.loop.run_until_complete(self.__async__mv2workdir())

    '''
    async def __async__mv2workdir(self):
        print('in async')
        return None
        for mount in self.hierarchy:
            if mount != self.working_dir:
                for f in os.listdir(mount):
                    await asyncio.sleep(60)
                    print('Moving file', f)
                    return move(os.path.join(mount, f), self.working_dir)
    '''


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
                
    
    def top_fs(self, size=1244*1.024*(10**6)):

        for mount in self.hierarchy:
            # must be at least 1MiB of space
            available = disk_usage(mount).free
            if mount in self.storage["tmpfs"]:
                tmpfs_used = sum(os.path.getsize(os.path.join(dp, f))
                                 for m in self.storage["tmpfs"]
                                 for dp, _, files in os.walk(m)
                                 for f in files
                                 if os.path.isfile(os.path.join(dp, f)))
                #print("Used tmpfs space:", tmpfs_used)
                try:
                    p = subprocess.Popen(["echo", os.environ["SLURM_MEM_PER_NODE"]],
                                          stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    (out, err) = p.communicate()
                    available_mem = int(int(out)*1.049*(10**6) - tmpfs_used)

                    if available_mem < available:
                        available = available_mem
                    logging.debug("Available space {}".format(available))
                except Exception as e:
                    pass
                    #print(str(e))
            if available > size:
                #print(mount, disk_usage(mount).free)
                return mount

        logging.error("Not enough space on any device")
        #sys.exit(1)


    def cleanup(self):
        logging.info('***Cleaning up FUSE fs***')
        for mount in self.hierarchy:
            if mount != self.working_dir:
                for f in os.listdir(mount):
                    fp = os.path.join(mount, f)

                    if f not in os.listdir(self.working_dir): 
                        logging.info('Moving file {} --> {}'.format(fp, self.working_dir))
                        move(os.path.join(mount, f), self.working_dir)

    
    # Filesystem methods
    # ==================

    def access(self, path, mode):
        full_path = self._full_path(path)
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)

    #TODO: adapt for directories
    def chmod(self, path, mode):
        full_path = self._full_path(path)
        return os.chmod(full_path, mode)

    #TODO: adapt for directories
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
            logging.error("{} does not exist.".format(full_path))
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

    # TODO: test to verify proper functioning
    # modified
    def readlink(self, path):
        fp = self._full_path(path)
        pathname = os.readlink(fp)
        if pathname.startswith("/"):
            # Path name is absolute, sanitize it.
            return os.path.relpath(pathname, os.path.dirname(fp))
        else:
            return pathname

    def mknod(self, path, mode, dev):
        return os.mknod(self._full_path(path), mode, dev)

    # modified
    def rmdir(self, path):
        spath = path.lstrip("/")
        logging.info("Removing directory", spath)
        for d in self.hierarchy:
            os.rmdir(os.path.join(d, spath))
        #full_path = self._full_path(path)
        #return os.rmdir(full_path)

    # modified
    def mkdir(self, path, mode):
        spath = path.lstrip("/")
        logging.info("Creating directory", spath)
        for d in self.hierarchy:
            os.mkdir(os.path.join(d, spath), mode)

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

    #TODO: adapt for directories
    def rename(self, old, new):
        return os.rename(self._full_path(old), self._full_path(new))

    def link(self, target, name):
        return os.link(self._full_path(target), self._full_path(name))

    def utimens(self, path, times=None):
        return os.utime(self._full_path(path), times)

    # File methods
    # ============

    def open(self, path, flags):
        logging.info("Opening file {}".format(path))
        full_path = self._full_path(path)
        return os.open(full_path, flags)

    def create(self, path, mode, fi=None):
        full_path = self._full_path(path)
        logging.info("Creating file {}".format(full_path))
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
            
            #print('ERROR: ', str(e))

            next_mount = self.top_fs(os.path.getsize(fp))
            new_fp = os.path.join(next_mount, os.path.basename(fp))

            if fp != new_fp:
                #TODO: test what happens when there are consecutive failures
                logging.warning('Out of memory: {} --->'.format(fp, new_fp))
                move(fp, new_fp)
                new_fh = os.open(new_fp, os.O_CREAT | os.O_RDWR)
                os.dup2(new_fh, fh)
                fp = new_fp

        return out

    
    def truncate(self, path, length, fh=None):
        logging.info("Truncating file {}".format(path))
        full_path = self._full_path(path)
        with open(full_path, 'r+') as f:
            f.truncate(length)

    def flush(self, path, fh):
        logging.info("Flushing file {}".format(path))
        return os.fsync(fh)

    def release(self, path, fh):
        logging.warning('File {} to be converted to read-only'.format(path))
        os.chmod(fh, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        logging.info('Closing file {}'.format(path))
        return os.close(fh)

    def fsync(self, path, fdatasync, fh):
        return self.flush(path, fh)


def main(fuse_dir, save_dir, log, whitelist=None, blacklist=None):
    FUSE(HierarchicalFs(os.path.abspath(save_dir),
         log=log, whitelist=whitelist, blacklist=blacklist),
         fuse_dir, nothreads=False, foreground=True,
         big_writes=True, max_read=262144, max_write=262144)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('fuse_dir', type=str,
                        help="Empty directory to mount FUSE" )
    parser.add_argument('final_dir', type=str,
                        help="Shared directory to flush all application data to")
    parser.add_argument('-w', '--whitelist', type=str, default=None,
                        help="file that contains desired mountpoints")
    parser.add_argument('-b', '--blacklist', type=str, default=None,
                        help='file that contains mountpoints to ignore')
    parser.add_argument('-l', '--loglevel',
                        choices=['debug', 'info', 'warning',
                                 'error', 'critical'], default='info',
                        help="application log level")
    args = parser.parse_args()

    main(args.fuse_dir, args.final_dir, args.loglevel,
         args.whitelist, args.blacklist)

