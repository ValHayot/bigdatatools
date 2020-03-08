#!/bin/bash

increment() {

	fs=$1
	infile=$2
	outdir=$3
	it=$4
	delay=$5

	sudo /sbin/sysctl vm.drop_caches=3
	python increment.py $2 $3 $1 $4 $5

}

passthrough="/root/fuse/build/example/passthrough_fh"

fuse_mount="/root/mount"
cp -r inc_in /dev/shm/
mkdir /dev/shm/out
increment native-mem /dev/shm/inc_in/inc_1.nii /dev/shm/out 10 0
rm /dev/shm/out/*

${passthrough} $fuse_mount
increment fuse-mem "${passthrough}/dev/shm/inc_in/inc_1.nii" "$passthrough/dev/shm/out" 10 0
fusermount -u ${fuse_mount}
rm /dev/shm/out/*
rm -rf /dev/shm/inc_in

cp -r inc_in /root
mkdir /root/out
increment native-ssd /root/inc_in/inc_1.nii /root/out 10 0
rm /root/out/*

${passthrough} $fuse_mount
increment fuse-ssd "${passthrough}/root/inc_in/inc_1.nii" "$passthrough/root/inc_in/out" 10 0
fusermount -u ${fuse_mount}
rm /root/out/*
rm -rf /root/inc_in

