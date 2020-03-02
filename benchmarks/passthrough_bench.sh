#!/usr/bin/env bash


vary_size() {
        out_dir=$1
        vary_type=$2 # block or file
        log_file=$3
        bs=128

        if [ ${log_file} == 'block' ]
        then
            count=8192
        else
            count=64
        fi

        echo "bs,count,bw,rate" > $log_file

        for i in {1..10}
        do
            sudo /sbin/sysctl vm.drop_caches=3
            out_f="${out_dir}/outfile-${i}.out"
            echo "Creating output file ${out_f}"

            dd if=/dev/urandom of=$out_f bs="${bs}"KiB count=$count conv=fdatasync,notrunc status=progress iflag=fullblock 2> passthrough_bench.tmp 
            bandwidth=$(cat passthrough_bench.tmp | tail -n 1 | rev | cut -d ' ' -f 1-2 | rev | tr ' ' ,)
            echo "$bs,$count,$bandwidth">> $log_file
            cat passthrough_bench.tmp
            rm passthrough_bench.tmp
            echo "Removing output file ${out_f}"
            rm $out_f

            # if not varying the block size then we're varying the file size
            if [ ${log_file} == 'block' ]
            then
                bs=$((2 * bs))
                count=$((count / 2))
            else
                count=$((count * 2))
            fi
        done

}


size_benchmarks() {
    b_type=$1

    # benchmark to determine how different block sizes affect bandwidth
    # Memory
    # Native FS

    vary_size "/dev/shm" ${b_type} ${b_type:0:1}s_native_mem_bench.csv

    # Fuse passthrough
    # load fs
    fuse_mount="/home/centos/sea/src/mount"
    /home/centos/sea/src/passthrough_fh $fuse_mount
    vary_size "${fuse_mount}/dev/shm" ${b_type} ${b_type:0:1}s_fuse_mem_bench.csv
    fusermount -u ${fuse_mount}

    # SSD
    # Native FS

    vary_size "/mnt/valfiles" ${b_type} ${b_type:0:1}s_native_ssd_bench.csv

    # Fuse passthrough

    /home/centos/sea/src/passthrough_fh $fuse_mount
    vary_size "${fuse_mount}/mnt/valfiles" ${b_type} ${b_type:0:1}s_fuse_ssd_bench.csv
    fusermount -u ${fuse_mount}

}


size_benchmarks "file"
