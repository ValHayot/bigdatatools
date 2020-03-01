#!/usr/bin/env bash


vary_block_size() {
        out_dir=$1
        log_file=$2
        bs=128
        count=8192

        echo "#bs,count,bw,rate" > $log_file

        for i in {1..10}
        do
            sudo /sbin/sysctl vm.drop_caches=3
            out_f="${out_dir}/outfile-${i}.out"
            echo "Creating output file ${out_f}"
            bs=$((2 * bs))
            count=$((count / 2))
            dd if=/dev/urandom of=$out_f bs="${bs}"KiB count=$count conv=fdatasync,notrunc status=progress iflag=fullblock 2> passthrough_bench.tmp 
            bandwidth=$(cat passthrough_bench.tmp | tail -n 1 | rev | cut -d ' ' -f 1-2 | rev | tr ' ' ,)
            echo "$bs,$count,$bandwidth">> $log_file
            cat passthrough_bench.tmp
            rm passthrough_bench.tmp
            echo "Removing output file ${out_f}"
            rm $out_f
        done

}

block_size_benchmarks() {

    # benchmark to determine how different block sizes affect bandwidth
    # Memory
    # Native FS

    vary_block_size "/dev/shm" bs_native_mem_bench.csv

    # Fuse passthrough
    # load fs
    fuse_mount="/home/centos/sea/src/mount"
    /home/centos/sea/src/passthrough_fh $fuse_mount
    vary_block_size "${fuse_mount}/dev/shm" bs_fuse_mem_bench.csv
    fusermount -u ${fuse_mount}

    # SSD
    # Native FS

    vary_block_size "/mnt/valfiles" bs_native_ssd_bench.csv

    # Fuse passthrough

    /home/centos/sea/src/passthrough_fh $fuse_mount
    vary_block_size "${fuse_mount}/mnt/valfiles" bs_ssd_mem_bench.csv
    fusermount -u ${fuse_mount}

}

block_size_benchmarks

# benchmark to determine how varying file sizes affect bandwidth
# Memory
# Native FS

# Fuse


# SSD
# Native FS

# Fuse passthrough

