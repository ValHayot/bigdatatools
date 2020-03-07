#!/usr/bin/env bash


vary_size() {
        in_f=$1
        out_dir=$2
        vary_type=$3 # block or file
        log_file=$4
        bs=128

        if [ ${log_file} == 'block' ]
        then
            count=8192
        else
            count=64
        fi


        for i in {1..10}
        do
            if [ ${in_f} == "bench_in.nii" ]
            then
                python gen_nifti.py $bs $count 
            fi

            sudo /sbin/sysctl vm.drop_caches=3
            out_f="${out_dir}/outfile-${i}.out"
            echo "Creating output file ${out_f}"

            dd if=$in_f of=$out_f bs="${bs}"KiB count=$count conv=fdatasync,notrunc status=progress iflag=fullblock 2> passthrough_bench.tmp 

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
                count=$((count + 1024))
            fi
        done

}


size_benchmarks() {
    in_f=$1
    b_type=$2
    increase=$3
    reps=$4
    passthrough=$5

    if [ ${in_f} == 'bench_in.nii' ]
    then
        prefix="nifti"
    else
        prefix="urandom"
    fi

    log_native_mem=$increase/${prefix}_${b_type:0:1}s_native_mem_bench.csv
    log_fuse_mem=${increase}/${prefix}_${b_type:0:1}s_fuse_mem_bench.csv
    log_native_ssd=${increase}/${prefix}_${b_type:0:1}s_native_ssd_bench.csv
    log_fuse_ssd=${increase}/${prefix}_${b_type:0:1}s_fuse_ssd_bench.csv

    #echo "bs,count,bw,rate" > ${log_native_mem}
    #echo "bs,count,bw,rate" > ${log_fuse_mem}
    #echo "bs,count,bw,rate" > ${log_native_ssd}
    #echo "bs,count,bw,rate" > ${log_fuse_ssd}

    for i in $(seq 1 ${reps}) 
    do
        echo $i ${reps}
        # benchmark to determine how different block sizes affect bandwidth
        # Memory
        # Native FS

        vary_size ${in_f} "/dev/shm" ${b_type} ${log_native_mem}
        # Fuse passthrough
        # load fs
        fuse_mount="/root/mount"
       	${passthrough} $fuse_mount
        vary_size ${in_f} "${fuse_mount}/dev/shm" ${b_type} ${log_fuse_mem} 
        fusermount -u ${fuse_mount}

        # SSD
        # Native FS

        vary_size ${in_f} "/root/tmp" ${b_type} ${log_native_ssd} 
        # Fuse passthrough

        ${passthrough} $fuse_mount
     	vary_size ${in_f} "${fuse_mount}/root/tmp" ${b_type} ${log_fuse_ssd}
        fusermount -u ${fuse_mount}
    done

}


size_benchmarks "bench_in.nii" "file" "linear" 5 /root/fuse/build/example/passthrough_fh
