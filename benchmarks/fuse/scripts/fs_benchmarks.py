import sys
import random
import os
import shutil
import subprocess
import pathlib

# sample command
# python fs_benchmarks.py ../../disk/scripts/bench_disks.sh 10 ../data/fusebenchmarks.csv /home/vhs/hierarchy_file.txt False
script = sys.argv[1]
iterations = sys.argv[2]
benchmark_file = sys.argv[3]
h_file = sys.argv[4]
pass_options = bool(sys.argv[5] == 'True')
print(pass_options)

pass_mount = '/dev/shm/passmount'
passhp_mount = '/dev/shm/passhpmount'
sea_mount = '/dev/shm/seamount'
sea_shared = '/mnt/lustre/vhs/seashared'

filesystems = ['/home/vhs/libfuse/build/example/passthrough_fh', '/home/vhs/libfuse/build/example/passthrough_hp', '/home/vhs/sea/src/sea', 'native']
mountpoints = ['/mnt/lustre/vhs/passthrough', '/tmp/passthrough', '/dev/shm/passthrough']

options = ['-o', 'kernel_cache', '-o', 'auto_cache', '-o', 'remember=1']

# create benchmark_file
with open(benchmark_file, 'w+') as f:
    f.write('device,mode,bandwidth,unit,source\n')

# Setup and randomize conditions for execution
conditions = [(fs, '{0}{1}'.format(m, fs[-3:]) if 'native' not in fs else m.replace('passthrough', 'native'))
              for fs in filesystems for m in mountpoints]
conditions *= 10
random.shuffle(conditions)

def start_fuse(fs, source, mount_type='fuse'):
    print('Starting FUSE')

    cmd = [fs]

    if '_hp' in fs:
        cmd.extend([source, passhp_mount])
    elif mount_type == 'fuse' and '_hp' not in fs:
        if pass_options:
            cmd.extend(options)
        cmd.append(pass_mount)
    else:
        cmd.extend(['--hierarchy_file={}'.format(h_file), sea_shared, sea_mount])

    print('Starting fs with command: ', ' '.join(cmd))

    p = subprocess.Popen(cmd, )


def stop_fuse(mountpoint=None):
    print('Stopping FUSE')
    if mountpoint is None:
        mountpoint = sea_mount

    p = subprocess.Popen(['fusermount3', '-u', mountpoint])
    p.communicate()


def run_benchmark(script, fs, mountpoint, benchmark_file):

    print('Running benchmarks')
    f = None
    dd_f = 'benchtest.txt'

    if 'passthrough_fh' in fs:
        f = '{0}{1}'.format(pass_mount, mountpoint)
        print(f)
        f = os.path.join(f, dd_f)
    elif 'passthrough_hp' in fs:
        f = os.path.join(passhp_mount, dd_f)
    elif 'sea' in fs:
        f = os.path.join(sea_mount, dd_f)
    else:
        f = os.path.join(mountpoint, dd_f)

    p = subprocess.Popen([script, f, benchmark_file, mountpoint], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (out, err) = p.communicate()

def cleanup_sea():

    print('Cleaning out files in mounted sea directories')
    with open(h_file, 'r') as f:
        for m in f:
            mount = m.strip(os.linesep)
            print('mountpoint', mount)
            files = list(pathlib.Path(mount).glob('*'))
            for f in files:
                print(f)
                os.remove(f)
        

for c in conditions:

    print(c)
    os.makedirs(c[1])
    if 'sea' in c[0]:
        start_fuse(*c, mount_type='sea')
        run_benchmark(script, *c, benchmark_file)
        stop_fuse(sea_mount)
        cleanup_sea()

    elif 'native' not in c[0]:
        start_fuse(*c)
        run_benchmark(script, *c, benchmark_file)
        if '_hp' in c[0]:
            stop_fuse(passhp_mount)
        else:
            stop_fuse(pass_mount)

    else:
        run_benchmark(script, *c, benchmark_file)
    print('Cleanup')
    shutil.rmtree(c[1])

