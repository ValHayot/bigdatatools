#!/usr/bin/env python

import os
import sys
from errno import *
from stat import *
import fcntl
import stat
import atexit
import signal
import subprocess
import logging
try:
    import _find_fuse_parts
except ImportError:
    pass
import fuse
from fuse import Fuse
from psutil import disk_partitions, disk_usage, virtual_memory
from blkinfo import BlkDiskInfo
from numpy import asarray
from getpass import getuser
from socket import gethostname
from time import time, sleep
from shutil import move, copy2, rmtree
from threading import Lock, Thread
from multiprocessing import Process
import asyncio

if not hasattr(fuse, '__version__'):
    raise RuntimeError("your fuse-py doesn't know of fuse.__version__, probably it's too old.")

fuse.fuse_python_api = (0, 2)
fuse.feature_assert('stateful_files', 'has_init')

def flag2mode(flags):
    md = {os.O_RDONLY: 'rb', os.O_WRONLY: 'wb', os.O_RDWR: 'wb+'}
    m = md[flags & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)]

    if flags | os.O_APPEND:
        m = m.replace('w', 'a', 1)

    return m


def add_el(d, k, v):
    if k in d and v not in d[k]:
        d[k].append(v)
    else:
        d[k] = [v]


def get_mount(all_partitions, fp):
    return max([(p.mountpoint, p.fstype, os.path.basename(p.device)
                 if p.device.startswith("/") else p.device)
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

    wd_mount, wd_fs, wd_pd = get_mount(all_partitions, working_dir)

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

            if mountpoint != wd_mount:
                mountpoint = os.path.join(mountpoint,
                                     '{0}-{1}'.format(gethostname(), getuser()))
            else:
                continue


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

            pd = parent_drives(disk_tree, wd_pd)
            fs = wd_fs

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




class HFS(Fuse):

    def __init__(self, *args, **kw):

        Fuse.__init__(self, *args, **kw)

        self.whitelist = None
        self.blacklist = None

    def start(self):

        # Only necessary for testing purposed
        if not hasattr(self, 'log'):
            self.log = "DEBUG"

        if not hasattr(self, 'root'):
            self.root = os.getcwd()

        # hardcoded for now
        self.alpha = 0.1

        numeric_lvl = getattr(logging, self.log.upper())

        fmt = '%(asctime)s:%(levelname)s:%(message)s'
        datefmt = '%d/%m/%Y %I:%M:%S %p'
        logging.basicConfig(format=fmt,
                            datefmt=datefmt,
                            level=numeric_lvl)

        self.logger = logging.getLogger('hfs')

        fh = logging.FileHandler('hfs.log')
        fh.setLevel(numeric_lvl)

        ch = logging.StreamHandler()
        ch.setLevel(numeric_lvl)

        formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

        self.logger.info("Setting up storage")

        self.storage = avail_fs(working_dir=self.root, whitelist=self.whitelist)
        self.possible_fs = self.storage.keys()
        self.hierarchy = self._sorted_storage()

        #print("\n".join("{0}: {1}".format(k, v) for k,v in self.storage.items()))

        self.logger.debug("Storage hierarchy: {}".format(" -> ".join(self.hierarchy)))

        self.process = Process(target=self._flush)
        self.process.start()

        self.process = Process(target=self._evict)
        self.process.start()
        #self.thread = Thread(target=self._flush)
        #self.thread.setDaemon(True)
        #self.thread.start()

        #logging.debug("Starting up async thread to cleanup LRU")
        #asyncio.run(self._evict())
        #self.thread = Thread(target=self._cleanup_tmp)
        #self.thread.setDaemon(True)
        #self.thread.start()

        #TODO: does not currently work
        #atexit.register(self._cleanup)
        signal.signal(signal.SIGTERM, self._cleanup)
        signal.signal(signal.SIGINT, self._cleanup)


    # modified
    def _full_path(self, partial):
        partial = partial.lstrip("/")

        paths = self._path_exists(partial)
        if len(paths) == 0 :
            paths.append(os.path.join(self._top_fs(), partial))
        return paths

    # modified
    def _path_exists(self, partial):
        paths = []

        for storage in self.hierarchy:
            fp = os.path.join(storage, partial)
            if os.path.exists(fp):
                paths.append(fp)
        return paths


    def _sorted_storage(self):
        priority_fs = []

        #TODO: convert to list comprehension
        for st in self.possible_fs:
            if st in self.storage:
                priority_fs.extend(self.storage[st])

        return priority_fs

    def _top_fs(self, size=1244*1.024*(10**6)):

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
                    self.logger.debug("Available space {}".format(available))
                except Exception as e:
                    pass
                    #print(str(e))
            if available > size:
                #print(mount, disk_usage(mount).free)
                return mount

        self.logger.error("Not enough space on any device")


    def _cleanup(self, *args, **kwargs):
        self.logger.info('***Cleaning up FUSE fs***')
        for mount in self.hierarchy:
            if mount != self.root:
                for f in os.listdir(mount):
                    fp = os.path.join(mount, f)

                    if f not in os.listdir(self.root):
                        self.logger.info('Moving file {} --> {}'.format(fp, self.root))
                        move(os.path.join(mount, f), self.root)

                self.logger.debug("Removing directory {}".format(mount))
                rmtree(mount)
        sys.exit(1)
        

    def _get_faccess(self, cp=False):
        file_access = {}
        for mount in self.hierarchy:
            if mount != self.root:
                for dirpath, _, files in os.walk(mount):
                    sd = os.path.relpath(dirpath, mount)
                    for f in files:
                        if cp and f in os.path.join(self.root, sd):
                            continue

                        fp = os.path.join(dirpath, f)
                        
                        # only cp if file is readonly
                        try:
                            if not os.access(fp, os.W_OK):
                                file_access[os.path.getatime(fp)] = (fp, sd)
                        except Exception as e:
                            self.logger.debug(str(e))


        return file_access


    def _flush(self, delay=0):
        #TODO: improve policy
        # remove file with oldest last access time
        logging.debug("Flusher process running")
        while True:
            #while True:
            sleep(delay)
            file_access = self._get_faccess(cp=True)

            if len(file_access.keys()) > 0:
                for fa, data in sorted(file_access.items(), key=lambda x: x[0]):
                    fp = data[0]
                    subdir = data[1]
                    out_fp = os.path.join(self.root, subdir.rstrip('.'),
                                           os.path.basename(fp))

                    #try:
                    if not os.path.isfile(out_fp) and os.path.isfile(fp):
                        self.logger.info('Copying file {} ---> {}'.format(fp, out_fp))
                        copy2(fp, out_fp)
                    #except Exception as e:
                    #    logging.debug('File {} move failed'.format(fp))
                    #    logging.debug(str(e))
                    

    # TODO: fix cleanup for when pipeline is known
    def _evict(self, init_delay=20, percent=60, reused_files=None):
        logging.debug("Eviction process started")
        while True:
            if virtual_memory().percent > percent:
                delay = 5
            else:
                delay = init_delay

            #sleep(delay)
            file_access = self._get_faccess()

            if len(file_access.keys()) > 0:
                fp, subdir = file_access[sorted(file_access.keys())[0]]
                fn = os.path.basename(fp)

                out_fn = os.path.join(self.root, subdir, fn)
                
                if (os.path.isfile(out_fn) and
                    (os.path.getsize(out_fn) == os.path.getsize(fp))):
                    logging.debug("Removing file {}".format(fp))
                    os.remove(fp)

    def getattr(self, path):
        full_path = self._full_path(path)
        return os.lstat(full_path[0])

    def readlink(self, path):
        full_path = self._full_path(path)
        return os.readlink(full_path[0])

    def readdir(self, path, offset):
        full_path = self._full_path(path)

        folder_dirs = []
        for p in full_path:
            for e in os.listdir(p):

                # Solution to make directories only appear once despite being
                # in multiple locations
                if os.path.isdir(os.path.join(p, e)):
                    if e in folder_dirs:
                        continue
                    folder_dirs.append(e)
                yield fuse.Direntry(e)

    def unlink(self, path):
        full_path = self._full_path(path)

        # Remove all occurrences of a given file
        for p in full_path:
            os.unlink(p)

    def rmdir(self, path):
        full_path = self._full_path(path)

        for p in full_path:
            self.logger.debug("Removing directory {}".format(p))
            os.rmdir(p)

    def symlink(self, path, path1):
        full_path1 = self._full_path(path1)
        os.symlink(path, full_path1[0])

    #TODO: This will likely change location of file to be renamed if memory is
    # full. need to fix
    def rename(self, path, path1):
        fp = self._full_path(path)

        if len(fp) > 1:
            for el in fp:
                p = el.replace(path, '')
                fp1 = os.path.join(p, path1)
                os.rename(el, fp1)
        else:
            os.rename(fp[0], fp1[0])

    def link(self, path, path1):
        fp = self._full_path(path)
        fp1 = self._full_path(path1)
        os.link(fp[0], fp1[0])

    def chmod(self, path, mode):
        fp = self._full_path(path)
        for el in fp:
            os.chmod(el, mode)

    def chown(self, path, user, group):
        fp = self._full_path(path)

        for el in fp:
            os.chown(el, user, group)

    def truncate(self, path, len):
        fp = self._full_path(path)
        f = open(fp[0], "a")
        f.truncate(len)
        f.close()

    def mknod(self, path, mode, dev):
        fp = self._full_path(path)
        os.mknod(fp[0], mode, dev)

    def mkdir(self, path, mode):
        for storage in self.hierarchy:
            fp_dir = os.path.join(storage, path.lstrip('/'))
            os.mkdir(fp_dir, mode)

    def utime(self, path, times):
        fp = self._full_path(path)

        for el in fp:
            os.utime(el, times)

    def access(self, path, mode):
        fp = self._full_path(path)
        if not os.access(fp[0], mode):
            return -EACCES

#    This is how we could add stub extended attribute handlers...
#    (We can't have ones which aptly delegate requests to the underlying fs
#    because Python lacks a standard xattr interface.)
#
#    def getxattr(self, path, name, size):
#        val = name.swapcase() + '@' + path
#        if size == 0:
#            # We are asked for size of the value.
#            return len(val)
#        return val
#
#    def listxattr(self, path, size):
#        # We use the "user" namespace to please XFS utils
#        aa = ["user." + a for a in ("foo", "bar")]
#        if size == 0:
#            # We are asked for size of the attr list, ie. joint size of attrs
#            # plus null separators.
#            return len("".join(aa)) + len(aa)
#        return aa

    def statfs(self):
        """
        Should return an object with statvfs attributes (f_bsize, f_frsize...).
        Eg., the return value of os.statvfs() is such a thing (since py 2.2).
        If you are not reusing an existing statvfs object, start with
        fuse.StatVFS(), and define the attributes.

        To provide usable information (ie., you want sensible df(1)
        output, you are suggested to specify the following attributes:

            - f_bsize - preferred size of file blocks, in bytes
            - f_frsize - fundamental size of file blcoks, in bytes
                [if you have no idea, use the same as blocksize]
            - f_blocks - total number of blocks in the filesystem
            - f_bfree - number of free blocks
            - f_files - total number of file inodes
            - f_ffree - nunber of free file inodes
        """

        # Return the status of the root directory only
        return os.statvfs(self.root)

    def fsinit(self):
        os.chdir(self.root)

    class HFSFile(object):

        storage = {}
        hierarchy = []
        _top_fs = None
        logger = None
        _full_path = None


        def __init__(self, path, flags, *mode):
            self.fp = self._full_path(path)[0]
            self.logger.debug("opening file {}".format(self.fp))
            self.file = os.fdopen(os.open(self.fp, flags, *mode),
                                  flag2mode(flags))
            self.fd = self.file.fileno()
            self.flags = flags
            self.mode = mode
            self.path = path
            self.wlock = Lock()

        def read(self, length, offset):
            self.logger.debug("reading file {}".format(self.fp))
            self.file.seek(offset)
            return self.file.read(length)

        def write(self, buf, offset):

            if self.wlock.acquire():
                #self.logger.debug("writing file {}".format(self.fp))
                buff_size = sys.getsizeof(buf)
                if self._top_fs(size=buff_size) in self.fp:
                    self.file.seek(offset)
                    self.file.write(buf)
                else:
                    tfs = self._top_fs(size=(os.path.getsize(self.fp) + buff_size))
                    new_fp = os.path.join(tfs, self.path.lstrip('/'))
                    
                    if new_fp != self.fp:
                        #self.file.flush()
                        #self.file.close()
                        #move(self.fp, new_fp)
                        #os.remove(self.fp)
                        #self.file = os.fdopen(os.open(new_fp, self.flags, *self.mode),
                        #                      flag2mode(self.flags))
                        self.logger.debug("Storage out of space. Changing file destination from {} to {}".format(self.fp, new_fp))
                        copy2(self.fp, new_fp)
                        new_fh = os.open(new_fp, os.O_CREAT | os.O_RDWR)
                        self.file = os.fdopen(new_fh, flag2mode(self.flags))
                        os.dup2(new_fh, self.fd)
                        self.fd = self.file.fileno()
                    elif new_fp != self.fp:
                        while not os.path.isfile(new_fp) and os.path.getsize(new_fp) < os.path.getsize(self.fp):
                            pass

                    #self.logger.debug(self.flags)
                    #self.logger.debug(self.mode)

                    #with open(self.fp, 'rb') as f:
                    #    self.file.seek(0)
                    #    data = f.read()
                    #    self.logger.debug("data length {}".format(sys.getsizeof(data)))
                    #    self.file.write(data)



                    self.file.seek(offset)
                    self.file.write(buf)

                    self.fp = new_fp
            self.wlock.release()

            return len(buf)

        def release(self, flags):
            self.logger.debug("releasing file {}".format(self.fp))
            self.logger.debug("removing old files")

            for f in self._full_path(self.path):
                if f != self.fp:
                    os.remove(f)
            self.file.close()

        def _fflush(self):
            if 'w' in self.file.mode or 'a' in self.file.mode:
                self.file.flush()

        def fsync(self, isfsyncfile):
            self.logger.debug("fsync file {}".format(self.fp))
            self._fflush()
            if isfsyncfile and hasattr(os, 'fdatasync'):
                os.fdatasync(self.fd)
            else:
                os.fsync(self.fd)

        def flush(self):
            self.logger.debug("flushing file {}".format(self.fp))
            self._fflush()

            if os.access(self.fp, os.W_OK):
                self.logger.warning('File {} to be converted to read-only'.format(self.path))
                os.chmod(self.fd, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)

            # cf. xmp_flush() in fusexmp_fh.c
            os.close(os.dup(self.fd))

        def fgetattr(self):
            self.logger.debug("get file {} attributes".format(self.path))
            return os.fstat(self.fd)

        def ftruncate(self, len):
            self.logger.debug("truncating file {}".format(self.path))
            self.file.truncate(len)

        def lock(self, cmd, owner, **kw):
            # The code here is much rather just a demonstration of the locking
            # API than something which actually was seen to be useful.

            # Advisory file locking is pretty messy in Unix, and the Python
            # interface to this doesn't make it better.
            # We can't do fcntl(2)/F_GETLK from Python in a platfrom independent
            # way. The following implementation *might* work under Linux.
            #
            # if cmd == fcntl.F_GETLK:
            #     import struct
            #
            #     lockdata = struct.pack('hhQQi', kw['l_type'], os.SEEK_SET,
            #                            kw['l_start'], kw['l_len'], kw['l_pid'])
            #     ld2 = fcntl.fcntl(self.fd, fcntl.F_GETLK, lockdata)
            #     flockfields = ('l_type', 'l_whence', 'l_start', 'l_len', 'l_pid')
            #     uld2 = struct.unpack('hhQQi', ld2)
            #     res = {}
            #     for i in xrange(len(uld2)):
            #          res[flockfields[i]] = uld2[i]
            #
            #     return fuse.Flock(**res)

            # Convert fcntl-ish lock parameters to Python's weird
            # lockf(3)/flock(2) medley locking API...
            op = { fcntl.F_UNLCK : fcntl.LOCK_UN,
                   fcntl.F_RDLCK : fcntl.LOCK_SH,
                   fcntl.F_WRLCK : fcntl.LOCK_EX }[kw['l_type']]
            if cmd == fcntl.F_GETLK:
                return -EOPNOTSUPP
            elif cmd == fcntl.F_SETLK:
                if op != fcntl.LOCK_UN:
                    op |= fcntl.LOCK_NB
            elif cmd == fcntl.F_SETLKW:
                pass
            else:
                return -EINVAL

            fcntl.lockf(self.fd, op, kw['l_start'], kw['l_len'])


    def main(self, *a, **kw):

        self.file_class = self.HFSFile
        try:
            self.file_class.storage = self.storage
            self.file_class.hierarchy = self.hierarchy
            self.file_class._top_fs = self._top_fs
            self.file_class.logger = self.logger
            self.file_class._full_path = self._full_path
        except Exception as e:
            print(str(e))

        return Fuse.main(self, *a, **kw)


def main():

    usage = """
Userspace nullfs-alike: mirror the filesystem tree from some point on.

""" + Fuse.fusage

    server = HFS(version="%prog " + fuse.__version__,
                 usage=usage,
                 dash_s_do='setsingle')

    server.parser.add_option(mountopt="root", metavar="PATH", default='/',
                             help="mirror filesystem from under PATH [default: %default]")
    server.parser.add_option(mountopt="log", choices=['DEBUG', 'INFO', 'WARNING', 'CRITICAL'], metavar="STR", default='DEBUG',
                             help="logging level")
    server.parser.add_option(mountopt="whitelist", type=str, default=None, help="File containing line-separated list of input mounts to consider")
    server.parser.add_option(mountopt="blacklist", type=str, default=None, help="File containing line-separated list of input mounts to ignore")
    server.parse(values=server, errex=1)

    try:
        if server.fuse_args.mount_expected():
            server.start()
            #os.chdir(server.root)
    except OSError as e:
        print("can't enter root of underlying filesystem", str(e))
        sys.exit(1)

    server.main()


if __name__ == '__main__':
    main()
