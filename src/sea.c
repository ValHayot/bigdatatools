/*
 * Sea File System
 * Adapted from Big Brother File System https://www.cs.nmsu.edu/~pfeiffer/fuse-tutorial/src/bbfs.c
 *
 */
#include "params.h"

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <libgen.h>
#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>

#ifdef HAVE_SYS_XATTR_H
#include <sys/xattr.h>
#endif


void sea_usage(){
    fprintf(stderr, "usage: sea [FUSE and mount options] rootDir mountPoint \n");
    abort();
}

int main(int argc, char *argv[])
{
    int fuse_stat;
    struct sea_state *sea_data;

    // check if root. if yes, abort ? TBD
    if ((getuid() == 0) || (geteuid() == 0)) {
            fprintf(stderr, "Sea cannot be executed as root. Terminating program\n");
            return 1;
    }
    
    fprintf(stderr, "FUSE library version %d.%d\n", FUSE_MAJOR_VERSION, FUSE_MINOR_VERSION);

    if ((argc < 3) || (argv[argc-2][0] == '-') || (argv[argc-1][0] == '-'))
        sea_usage();

    sea_data = malloc(sizeof(struct sea_state));

    if (sea_data == NULL) {
        perror("main calloc");
        abort();
    }



}
