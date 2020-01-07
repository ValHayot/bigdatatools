/*
 * Sea File System - params.h
 * Adapted from Big Brother File System https://www.cs.nmsu.edu/~pfeiffer/fuse-tutorial/src/bbfs.c
 *
 */
#ifndef _PARAMS_H_
#define _PARAMS_H_

#define FUSE_USE_VERSION 29

#define _XOPEN_SOURCE 500

#include <limits.h>
#include <stdio.h>
struct sea_state {
    char *rootdir;
};
#define SEA_DATA ((struct sea_state *) fuse_get_context()->private_data)

#endif


