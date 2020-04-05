#!/bin/bash

device=$1
bench_file=$2
echo 3 | sudo tee /proc/sys/vm/drop_caches

write_out=$((dd if=/dev/zero of=$device bs=1M count=1024 conv=fdatasync,notrunc status=progress)  2>&1)
write_bw=`echo ${write_out} | awk '{print $(NF-1),",",$NF}' | tr -d ' '`
echo ${device},write,${write_bw} >> ${bench_file}

echo 3 | sudo tee /proc/sys/vm/drop_caches
read_out=$((dd if=$device of=/dev/null bs=1M count=1024 status=progress) 2>&1)
read_bw=`echo ${read_out} | awk '{print $(NF-1),",",$NF}' | tr -d ' '`
echo ${device},read,${read_bw} >> ${bench_file}


cached_out=$((dd if=$device of=/dev/null bs=1M count=1024 status=progress) 2>&1)
cached_bw=`echo ${cached_out} | awk '{print $(NF-1),",",$NF}' | tr -d ' '`
echo ${device},cached,${cached_bw} >> ${bench_file}

rm $device

