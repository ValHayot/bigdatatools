/*
  FUSE: Filesystem in Userspace
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>
  Copyright (C) 2011       Sebastian Pipping <sebastian@pipping.org>

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.
*/

/** @file
 *
 * This file system mirrors the existing file system hierarchy of the
 * system, starting at the root file system. This is implemented by
 * just "passing through" all requests to the corresponding user-space
 * libc functions. This implementation is a little more sophisticated
 * than the one in passthrough.c, so performance is not quite as bad.
 *
 * Compile with:
 *
 *     gcc -Wall passthrough_fh.c `pkg-config fuse3 --cflags --libs` -lulockmgr -o passthrough_fh
 *
 * ## Source code ##
 * \include passthrough_fh.c
 */


// remove this in the future
#define HAVE_UTIMENSAT 1

#define FUSE_USE_VERSION 31

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#define _GNU_SOURCE

#include <fuse.h>

#ifdef HAVE_LIBULOCKMGR
#include <ulockmgr.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <unistd.h>
#include <libgen.h>
#include <fcntl.h>
#include <utime.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <dirent.h>
#include <errno.h>
#include <sys/time.h>
#include <assert.h>
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif
#include <sys/file.h> /* flock(2) */

#define NUM_MOUNTS 10
#define MAX_BLOCKS 100
#define WRITE_SIZE 131072 /* can get from fuse directly most likely */
#define SEA_CHUNK ".seapart_"


static struct options {
    const char *rootdir;
    const char *hierarchy_file;
    char hierarchy [NUM_MOUNTS][PATH_MAX+1];
    int show_help;
    int total_mounts;
} options;

#define OPTION(t, p)                 \
    {t, offsetof(struct options, p), 1 }
static const struct fuse_opt option_spec[] = {
    OPTION("--hierarchy_file=%s", hierarchy_file),
    OPTION("-h", show_help),
    OPTION("--help", show_help),
    FUSE_OPT_END
};

// from: https://cboard.cprogramming.com/c-programming/143155-simple-removal-string-string-via-strtok.html
static void stripstr(char *string, char *sub) {
    char *match = string;
    match = strstr(match, sub);
    *match = '\0';
}

static void get_fusepath(char blockname[PATH_MAX], char fusepath[PATH_MAX])
{

    if (strstr(blockname, SEA_CHUNK) != NULL)
    {
        char *path_dup_dir = strndup(blockname, PATH_MAX);
        char *path_dup_bn = strndup(blockname, PATH_MAX);
        char *dn = dirname(path_dup_dir);
        char *bn = basename(path_dup_bn);
        bn++;
        fprintf(stderr, "sea: bn %s\n", bn);
        stripstr(bn, SEA_CHUNK);
        fprintf(stderr, "sea: bn %s dn %s \n", bn, dn);

        char* tmp_dn = strndup(dn, PATH_MAX);
        if (strcmp(dn, ".") != 0)
        {
            fprintf(stderr, "sea: dirname %s\n", dn);
            strncat(fusepath, dn, PATH_MAX);
            strncpy(fusepath, bn, PATH_MAX);
        }
        else{
            strncpy(fusepath, bn, PATH_MAX);
        }
        fprintf(stderr, "sea: bn %s dn %s \n", bn, dn);
    }
    else
    {
        strcpy(fusepath, blockname);
    }
    fprintf(stderr, "sea: fusepath %s\n", fusepath);
}


static void sea_getblocks(char blocks[][PATH_MAX], const char *path)
{

    //initialize the blocks

    for(int i=0; i < MAX_BLOCKS; i++)
        strcpy(blocks[i], "");

    //get basename
    char *path_dup_dir = strndup(path, PATH_MAX);
    char *path_dup_bn = strndup(path, PATH_MAX);
    fprintf(stderr, "Sea: path dup %s\n", path_dup_bn);
    char *bn = basename(path_dup_bn);
    char *dn = dirname(path_dup_dir);
    fprintf(stderr, "Sea: path dirname %s\n", dn);
    fprintf(stderr, "Sea: path basename %s\n", bn);

    char fpath[PATH_MAX];

    int j = 0;
    for (int i = 0; i < options.total_mounts; i++){
        strcpy(fpath, options.hierarchy[i]);
        strncat(fpath, dn, PATH_MAX);

        DIR *dir;
        struct dirent *ent;
        if ((dir = opendir (options.hierarchy[i])) != NULL) {
          /* print all the files and directories within directory */
          while ((ent = readdir (dir)) != NULL) {

            if (strstr(ent->d_name, SEA_CHUNK) == NULL)
                continue;

            if ( strstr(ent->d_name, bn) != NULL ){
                strcpy(blocks[j], options.hierarchy[i]);
                strncat(blocks[j], "/", PATH_MAX);
                strncat(blocks[j], ent->d_name, PATH_MAX);
                fprintf(stderr, "Sea: block name %s\n", blocks[j]);
                j++;
            }
          }
          closedir (dir);
        } else {
            /* could not open directory */
            fprintf(stderr, "Sea: could not open directory in get blocks\n");
            abort();
            //return EXIT_FAILURE;
        }
    }

}

static void sea_fullpath(char fpath[PATH_MAX], const char *path)
{
    fprintf(stdout, "sea path: %s\n", path);
    int exists = 0;

    // get max inodes in directory
    //struct statvfs st;
    //if (statvfs(options.rootdir, &st) != 0) {
    //    fprintf(stderr, "Error getting mounts\n");
    //    abort();
    //}

    //if (st.f_favail == 0){
    //    fprintf(stderr, "insufficient inodes");
    //}

    // all possible locations for a given file/directory will be stored here
    //char ** all_paths = ( char ** ) malloc ( sizeof ( char * ) * options.total_mounts );
    
    //int j = 0;
    // Check if file already exists
    for (int i = 0; i < options.total_mounts; i++){
        strcpy(fpath, options.hierarchy[i]);
        strncat(fpath, path, PATH_MAX);

        //fprintf(stderr, "sea hierarchy %s\n", fpath);
        struct stat sb;
        if (stat(fpath, &sb) == 0){
            exists = 1;
            break;
            //all_paths[j] = ( char * ) malloc ( sizeof ( char ) * ( PATH_MAX + 1 ) );
            //strcpy(all_paths[j], fpath);
        }
    }

    
    // if file does not exist, check if blocks exist or return best avail fs
    if (exists == 0){
//        char blocks[options.total_mounts][PATH_MAX];
//
//        sea_getblocks(blocks, path);
//
//        if (blocks[0] == '\0') {
//            char* blocks_[0]
//        }


        for (int i = 0; i < options.total_mounts; i++){
            strcpy(fpath, options.hierarchy[i]);
            strncat(fpath, path, PATH_MAX);

            // Should add a check here to make sure working directory != current hierarch fs
            struct statvfs st_wd;
            struct statvfs st_m;
            if (statvfs(options.rootdir, &st_wd) != 0){
                fprintf(stderr, "Sea: sea_fullpath: error getting working directory %s info \n", options.rootdir);
                abort();
            }
            if (statvfs(options.hierarchy[i], &st_m) != 0){
                fprintf(stderr, "Sea: sea_fullpath: error getting mount directory %s info \n", options.hierarchy[i]);
                abort();
            }

            //fprintf(stderr, "Sea: sea_fullpath error: favail %d bavail %d favail %d bavail %d", st_wd.f_favail, st_wd.f_bavail, st_m.f_favail, st_m.f_bavail);
            if (st_wd.f_favail > 0 && st_wd.f_bavail > 0 && st_m.f_favail > 0 && st_m.f_bavail > 0){
                break;
            }
        }
    }
    fprintf(stderr, "sea fullpath: %s\n", fpath);

}

// same as sea_fullpath except will return all possible directory locations
static void sea_allpaths(char allpaths[][PATH_MAX], const char *path){
    fprintf(stdout, "Sea: get all mounts\n");
    
    int exists = 0;

    // initialize
    for(int i=0; i < options.total_mounts; i++)
        strcpy(allpaths[i], "");

    char fpath[PATH_MAX];

    for (int i = 0; i < options.total_mounts; i++){
        strcpy(fpath, options.hierarchy[i]);
        strncat(fpath, path, PATH_MAX);

        struct stat sb;
        if (stat(fpath, &sb) == 0){
            exists = 1;
            strcpy(allpaths[i], fpath);
        }
    }

    if (exists == 0){
        for (int i = 0; i < options.total_mounts; i++){
            strcpy(fpath, options.hierarchy[i]);
            strncat(fpath, path, PATH_MAX);

            // Should add a check here to make sure working directory != current hierarch fs
            struct statvfs st_wd;
            struct statvfs st_m;
            if (statvfs(options.rootdir, &st_wd) != 0 && statvfs(options.hierarchy[i], &st_m)) {
                fprintf(stderr, "Error getting working directory %s or fs %s info \n", options.rootdir, options.hierarchy[i]);
                abort();
            }

            if (st_wd.f_favail > 0 && st_wd.f_bavail > 0 && st_m.f_favail > 0 && st_m.f_bavail > 0){
                strcpy(allpaths[i], fpath);
            }
        }
    }
    

}

static void *sea_init(struct fuse_conn_info *conn,
		      struct fuse_config *cfg)
{
    fprintf(stderr, "Initializing Sea filesystem\n");
	(void) conn;
	cfg->use_ino = 1;
	cfg->nullpath_ok = 1;

	/* Pick up changes from lower filesystem right away. This is
	   also necessary for better hardlink support. When the kernel
	   calls the unlink() handler, it does not know the inode of
	   the to-be-removed entry and can therefore not invalidate
	   the cache of the associated inode - resulting in an
	   incorrect st_nlink value being reported for any remaining
	   hardlinks to this inode. */
	cfg->entry_timeout = 0;
	cfg->attr_timeout = 0;
	cfg->negative_timeout = 0;

    // Setting max write value to 128MB
    //conn->max_write = 134217728;

    FILE* fhierarchy = fopen(options.hierarchy_file, "r");
    fhierarchy = fopen(options.hierarchy_file, "r");
    if (fhierarchy == NULL){
        fprintf(stderr, "fuse: error opening hierarchy file: %s\n", options.hierarchy_file);
        exit(1);
    }

    int i = 0;
    fhierarchy = fopen(options.hierarchy_file, "r");
    while (fgets(options.hierarchy[i], sizeof(options.hierarchy[i]), fhierarchy) != NULL){

        fprintf(stderr, "%d\n", i);
        // Strip newline character from filename string if present
        int len = strlen(options.hierarchy[i]);
        if (len > 0 && options.hierarchy[i][len-1] == '\n')
            options.hierarchy[i][len - 1] = 0;

        struct stat sb;

        if (stat(options.hierarchy[i], &sb) != 0 || !(sb.st_mode & S_IFDIR)){
            fprintf(stderr, "Invalid mountpoint: %s\n", options.hierarchy[i]);
            // Need to figure out how to safely exit with error
            fclose(fhierarchy);
            exit(1);
        }
        i++;
    }
    fclose(fhierarchy);

    options.total_mounts = i;
    
    fprintf(stdout, "===Sea mount hierarchy===\n\n");
    for (int i = 0; i < options.total_mounts ; i++){
        fprintf(stdout, "Level %d: %s\n", i, options.hierarchy[i]);
    }
    fprintf(stdout, "\n=========================\n");

    fprintf(stderr, "Sea filesystem initialized\n");
	return NULL;
}

// struct for file object as they'll be partitioned across file systems
struct sea_filep {
	int fd;
    char main_path[PATH_MAX];
    char curr_path[PATH_MAX];
	off_t offset;
    off_t curr_offset;
    int block_count;
    int create_flags;
};

static inline struct sea_filep *get_filep(struct fuse_file_info *fi)
{
	return (struct sea_filep *) (uintptr_t) fi->fh;
}

static int sea_getattr(const char *path, struct stat *stbuf,
			struct fuse_file_info *fi)
{
    fprintf(stderr, "Sea: getattr of path %s\n", path);
	int res = 0;

	(void) path;

    char *empty_str = "";
	if(fi){
	    struct sea_filep *f = get_filep(fi);
		res = fstat(f->fd, stbuf);

        // probably can take this out of if/else
        char blocks[MAX_BLOCKS][PATH_MAX];

        fprintf(stderr, "Sea: getattr of path main path %s\n", f->main_path);
        sea_getblocks(blocks, f->main_path);

        for (int i=1; strcmp(blocks[i], empty_str); i++){
            struct stat *tempbuf = malloc(sizeof(struct stat));
            fprintf(stderr, "sea: current getattr current block %s\n", blocks[i]);

            lstat(blocks[i], tempbuf);

            stbuf->st_size += tempbuf->st_size;
            stbuf->st_blocks += tempbuf->st_blocks;
        }


    }
	else
    {
        fprintf(stderr, "Sea: getattr from filepath\n");
        char blocks[MAX_BLOCKS][PATH_MAX];
        sea_getblocks(blocks, path);
        fprintf(stderr, "Sea: blocks obtained %s\n", blocks[0]);


        if (strcmp(blocks[0], empty_str) != 0)
        {
            fprintf(stderr, "sea: getting attribute of block %s\n", blocks[0]);
            res = lstat(blocks[0], stbuf);
            for (int i=1; strcmp(blocks[i], empty_str); i++){
                struct stat *tempbuf = malloc(sizeof(struct stat));

                lstat(blocks[i], tempbuf);

                stbuf->st_size += tempbuf->st_size;
                stbuf->st_blocks += tempbuf->st_blocks;
            }
            fprintf(stderr, "sea: result of getting attribute of block %d\n", res);
        }
        else
        {
            char fpath[PATH_MAX];
            sea_fullpath(fpath, path);
            res = lstat(fpath, stbuf);
        }
    }
	if (res < 0)
		return -errno;

    fprintf(stderr, "Sea: getattr of path completed %s %lu\n", path, stbuf->st_size);
	return res;
}

static int sea_access(const char *path, int mask)
{
    fprintf(stderr, "Sea: access of path %s\n", path);
	int res = 0;

    char fpath[PATH_MAX];
    sea_fullpath(fpath, path);
    fprintf(stderr, "Sea: access of full path %s\n", fpath);
	res = access(fpath, mask);
    fprintf(stderr, "Sea: access of path %s completed.\n", path);
	if (res < -1)
		return -errno;

	return res;
}

static int sea_readlink(const char *path, char *buf, size_t size)
{
    fprintf(stderr, "Sea: readlink of path %s\n", path);
	int res = 0;

    char fpath[PATH_MAX];
    sea_fullpath(fpath, path);

	res = readlink(fpath, buf, size - 1);
	if (res < 0)
		return -errno;

	buf[res] = '\0';

    fprintf(stderr, "Sea: readlink of path %s completed.\n", path);
	return res;
}

struct sea_dirp {
	DIR *dp;
	struct dirent *entry;
	off_t offset;
    DIR **alldps;
    char allpaths[1][PATH_MAX];
};

static int sea_opendir(const char *path, struct fuse_file_info *fi)
{
    fprintf(stderr, "sea: opening directory at path: %s\n", path);
	int res = 0;
	struct sea_dirp *d = malloc(sizeof(struct sea_dirp) + (options.total_mounts - 1)*PATH_MAX);
    d->alldps = malloc(sizeof(DIR**)*options.total_mounts);


	if (d == NULL)
		return -ENOMEM;

    sea_allpaths(d->allpaths, path);

    for (int i = 0; i < options.total_mounts; i++){
        fprintf(stdout, "sea: opening directory %s\n", d->allpaths[i]);

        d->dp = opendir(d->allpaths[i]);
        if (d->dp == NULL) {
            res = -errno;
            free(d);
            return res;
        }
        else{
            d->alldps[i] = d->dp;
            fprintf(stderr, "sea: i %d alldps %d",i, d->alldps[i]);
        }
        d->offset = 0;
        d->entry = NULL;

        fi->fh = (unsigned long) d;

        fprintf(stderr, "sea: directory %s opened %lu\n", d->allpaths[i], (unsigned long)d->alldps[i]);
    }
	return res;
}

static inline struct sea_dirp *get_dirp(struct fuse_file_info *fi)
{
	return (struct sea_dirp *) (uintptr_t) fi->fh;
}

static int sea_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
		       off_t offset, struct fuse_file_info *fi,
		       enum fuse_readdir_flags flags)
{
	struct sea_dirp *d = get_dirp(fi);
    fprintf(stderr, "sea: reading directory %s\n", d->allpaths[0]);
    fprintf(stderr, "dir p %lu %lu", (unsigned long) d->dp, (unsigned long) d->alldps[0]);

	(void) path;

    if (offset != d->offset) {
        for(int i = 0; i < options.total_mounts; i++){
            if(d->alldps[i] == NULL)
                break;

#ifndef __FreeBSD__
            seekdir(d->alldps[i], offset);
#else
            /* Subtract the one that we add when calling
               telldir() below */
            seekdir(d->alldps[i], offset-1);
#endif
        }
        d->entry = NULL;
        d->offset = offset;
    }
    for(int i=0; i < options.total_mounts; i++){

        if(d->alldps[i] == NULL)
            break;

        while (1) {
            struct stat st;
            off_t nextoff;
            enum fuse_fill_dir_flags fill_flags = 0;

            if (!d->entry) {
                d->entry = readdir(d->alldps[i]);
                if (!d->entry)
                    break;
            }
#ifdef HAVE_FSTATAT
            if (flags & FUSE_READDIR_PLUS) {
                int res;

                res = fstatat(dirfd(d->alldps[i]), d->entry->d_name, &st,
                          AT_SYMLINK_NOFOLLOW);
                if (res != -1)
                    fill_flags |= FUSE_FILL_DIR_PLUS;
            }
#endif
            if (!(fill_flags & FUSE_FILL_DIR_PLUS)) {
                memset(&st, 0, sizeof(st));
                st.st_ino = d->entry->d_ino;
                st.st_mode = d->entry->d_type << 12;
            }
            nextoff = telldir(d->alldps[i]);
#ifdef __FreeBSD__		
            /* Under FreeBSD, telldir() may return 0 the first time
               it is called. But for libfuse, an offset of zero
               means that offsets are not supported, so we shift
               everything by one. */
            nextoff++;
#endif
            char fusepath[PATH_MAX];

            get_fusepath(d->entry->d_name, fusepath);

            char first_chunk[PATH_MAX];
            sprintf(first_chunk, "%s0", SEA_CHUNK);

            //TODO: Add . and ..
            if (strstr(d->entry->d_name, first_chunk) && filler(buf, fusepath, &st, nextoff, fill_flags))
                break;

            fprintf(stderr, "DIRENTRY %s\n", d->entry->d_name);
            d->entry = NULL;
            d->offset = nextoff;
        }
    }
    fprintf(stderr, "sea: directory %s read\n", d->allpaths[0]);
	return 0;
}

static int sea_releasedir(const char *path, struct fuse_file_info *fi)
{
    fprintf(stderr, "sea: releasing directory at path: %s\n", path);
	struct sea_dirp *d = get_dirp(fi);
	(void) path;
	closedir(d->dp);
	free(d);
    fprintf(stderr, "sea: directory %s released\n", path);
	return 0;
}

static int sea_mknod(const char *path, mode_t mode, dev_t rdev)
{
    fprintf(stderr, "Sea: mknod of path %s\n", path);
	int res = 0;

    char fpath[PATH_MAX];
    sea_fullpath(fpath, path);

	if (S_ISFIFO(mode))
		res = mkfifo(fpath, mode);
	else
		res = mknod(fpath, mode, rdev);
	if (res < 0)
		return -errno;

    fprintf(stderr, "Sea: mknod of path %s completed.\n", path);

	return res;
}

static int sea_mkdir(const char *path, mode_t mode)
{
    fprintf(stderr, "sea: creating directory at path: %s\n", path);
	int res = 0;
    char fpath[PATH_MAX];
    sea_fullpath(fpath, path);

	res = mkdir(fpath, mode);
	if (res < 0)
		return -errno;

    fprintf(stderr, "sea: created directory at path: %s\n", fpath);

	return res;
}

static int sea_unlink(const char *path)
{
    fprintf(stderr, "Sea: unlink of path %s\n", path);
	int res = 0;
    char fpath[PATH_MAX];
    sea_fullpath(fpath, path);

	res = unlink(fpath);
	if (res < 0)
		return -errno;

    fprintf(stderr, "Sea: unlink of path %s completed.\n", path);

	return res;
}

static int sea_rmdir(const char *path)
{
    fprintf(stderr, "sea: removing directory at path: %s\n", path);
	int res = 0;
    char fpath[PATH_MAX];
    sea_fullpath(fpath, path);

	res = rmdir(fpath);
	if (res < 0)
		return -errno;

    fprintf(stderr, "sea: removed directory at path: %s\n", fpath);

	return res;
}

static int sea_symlink(const char *from, const char *to)
{
    fprintf(stderr, "sea: symlink of path %s to %s\n", from, to);
	int res = 0;

	res = symlink(from, to);
	if (res < 0)
		return -errno;

    fprintf(stderr, "sea: symlink of path %s to %s completed.\n", from, to);
	return res;
}

static int sea_rename(const char *from, const char *to, unsigned int flags)
{
    fprintf(stderr, "sea: renaming directory %s to %s\n", from, to);
	int res = 0;

	/* When we have renameat2() in libc, then we can implement flags */
	if (flags)
		return -EINVAL;

	res = rename(from, to);
	if (res < 0)
		return -errno;

    fprintf(stderr, "sea: renaming directory %s to %s completed.\n", from, to);
	return res;
}

static int sea_link(const char *from, const char *to)
{
    fprintf(stderr, "sea: link directory %s to %s\n", from, to);
	int res = 0;

	res = link(from, to);
	if (res < 0)
		return -errno;

    fprintf(stderr, "sea: link directory %s to %s completed.\n", from, to);

	return res;
}

static int sea_chmod(const char *path, mode_t mode,
		     struct fuse_file_info *fi)
{
    fprintf(stderr, "sea: chmod path  %s\n", path);
	int res = 0;

	if(fi)
		res = fchmod(fi->fh, mode);
	else
    {
        char fpath[PATH_MAX];
        sea_fullpath(fpath, path);
		res = chmod(fpath, mode);
    }
	if (res < 0)
		return -errno;

    fprintf(stderr, "sea: chmod path  %s completed.\n", path);
	return res;
}

static int sea_chown(const char *path, uid_t uid, gid_t gid,
		     struct fuse_file_info *fi)
{
    fprintf(stderr, "sea: chown path  %s\n", path);
	int res = 0;

	if (fi)
		res = fchown(fi->fh, uid, gid);
	else
    {
        char fpath[PATH_MAX];
        sea_fullpath(fpath, path);
		res = lchown(fpath, uid, gid);
    }
	if (res < 0)
		return -errno;

    fprintf(stderr, "sea: chown path %s completed.\n", path);
	return res;
}

static int sea_truncate(const char *path, off_t size,
			 struct fuse_file_info *fi)
{
    fprintf(stderr, "sea: truncate path  %s\n", path);
	int res = 0;

	if(fi)
		res = ftruncate(fi->fh, size);
	else{
        char fpath[PATH_MAX];
        sea_fullpath(fpath, path);
		res = truncate(fpath, size);
    }
	if (res < 0)
		return -errno;

    fprintf(stderr, "sea: truncate path  %s completed.\n", path);
	return res;
}

#ifdef HAVE_UTIMENSAT
static int sea_utimens(const char *path, const struct timespec ts[2],
		       struct fuse_file_info *fi)
{
    fprintf(stderr, "Sea: setting time of path %s\n", path);
	int res = 0;

	/* don't use utime/utimes since they follow symlinks */
	if (fi)
		res = futimens(fi->fh, ts);
	else
    	{
       	    char fpath[PATH_MAX];
        	sea_fullpath(fpath, path);
		
		res = utimensat(0, fpath, ts, AT_SYMLINK_NOFOLLOW);
    	}
	if (res < 0)
		return -errno;

    fprintf(stderr, "Sea: setting time of path %s completed.\n", path);
	return res;
}
#endif

static void get_block_name(char block_path[PATH_MAX], const char *path, off_t offset)
{
    char* path_dup_bn = strndup(path, PATH_MAX);
    char* path_dup_dn = strndup(path, PATH_MAX);
    char block_name[PATH_MAX];
    char hidden[PATH_MAX];
    char suffix[PATH_MAX];

    sprintf(suffix, "%s%lld", SEA_CHUNK, (long long int)offset);

    strcpy(block_path, dirname(path_dup_dn));
    strcpy(block_name, basename(path_dup_bn));
    strcpy(hidden, ".");
    strncat(hidden, block_name, PATH_MAX);
    strncat(block_path, hidden, PATH_MAX);
    strncat(block_path, suffix, PATH_MAX); 
    fprintf(stderr, "Sea: hidden %s, path %s, block_name %s, block_path %s\n", hidden, path, block_name, block_path);
}

static int sea_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    char fpath[PATH_MAX];
    char block_path[PATH_MAX]; 
    get_block_name(block_path, path, 0);
    sea_fullpath(fpath, block_path);

    struct sea_filep *f = malloc(sizeof(struct sea_filep));
    strcpy(f->main_path, path);
    
    fprintf(stderr, "sea create fp: %s\n", fpath);
	f->fd = open(fpath, fi->flags, mode);
	if (f->fd == -1)
		return -errno;

    f->create_flags = fi->flags;
    f->offset = 0;
    f->curr_offset = 0;
    f->block_count = 1;
    strcpy(f->curr_path, fpath);

	fi->fh = (unsigned long) f;

    fprintf(stderr, "Sea: created path %s\n", fpath);
	return 0;
}

static int sea_open(const char *path, struct fuse_file_info *fi)
{

    fprintf(stderr, "Sea: attempting to open path %s\n", path);
    char fpath[PATH_MAX];
    char block_path[PATH_MAX]; 

    get_block_name(block_path, path, 0);
    sea_fullpath(fpath, block_path);

    struct sea_filep *f = malloc(sizeof(struct sea_filep));

    strcpy(f->main_path, path);

    if (f == NULL)
        return -ENOMEM;

    fprintf(stderr, "sea open fp: %s\n", fpath);
	f->fd = open(fpath, fi->flags);
	if (f->fd == -1)
		return -errno;

    f->offset = 0;
    f->curr_offset = 0;
    f->block_count = 1;
    strcpy(f->curr_path, fpath);

	fi->fh = (unsigned long) f;
    fprintf(stderr, "Sea: open path %s completed.\n", fpath);
	return 0;
}

static int sea_read(const char *path, char *buf, size_t size, off_t offset,
		    struct fuse_file_info *fi)
{
    fprintf(stderr, "Sea: read path %s\n", path);
	int res = 0;

    //
    struct sea_filep *f = get_filep(fi);
    char blocks[MAX_BLOCKS][PATH_MAX];
    sea_getblocks(blocks, path);

    for(int i=0; i < options.total_mounts; i++){
        char* block_offset = strchr(blocks[1], '_');
        off_t b_off = (off_t) block_offset;

        if (offset >= b_off && offset - b_off <= size){
            int fd = open(blocks[i], fi->flags);
            size_t adjusted_size = size - offset + b_off; 
            res = pread(fd, buf, adjusted_size, offset - b_off);
        }

    }
	if (res < 0)
		res = -errno;

    fprintf(stderr, "Sea: read path %s completed.\n", path);

	return res;
}

static int get_numblocks(char blocks[][PATH_MAX]){
    int num = 0;
    char *empty_str = "";
    for(; strcmp(blocks[num], empty_str); num++ );
    return num;
}

static int sea_read_buf(const char *path, struct fuse_bufvec **bufp,
			size_t size, off_t offset, struct fuse_file_info *fi)
{
    struct sea_filep *f = get_filep(fi);

    fprintf(stderr, "Sea: read buff of path %s\n", f->main_path);
	struct fuse_bufvec *src;

	(void) path;

    char blocks[MAX_BLOCKS][PATH_MAX];
    sea_getblocks(blocks, f->main_path);
    int n_blocks = get_numblocks(blocks);

	src = malloc(sizeof(struct fuse_bufvec)*n_blocks);

	if (src == NULL)
		return -ENOMEM;

	*src = FUSE_BUFVEC_INIT(size);

    struct stat *st = malloc(sizeof(struct stat));
    lstat(blocks[0], st);

    size_t readsize = 0;

    if (size > st->st_size)
    {
        readsize = st->st_size;
    }
    else
        readsize = size;

    src->buf[0].size = readsize;
    src->buf[0].flags = FUSE_BUF_IS_FD | FUSE_BUF_FD_SEEK;
    src->buf[0].fd = f->fd;
    src->buf[0].pos = 0;

    fprintf(stderr, "sea: size to read %lu\n", size);

    for (int i=1; i < n_blocks; i++){
        fprintf(stderr, "sea: reading current buff %s\n", blocks[i]);
        int fd = open(blocks[i], fi->flags);
        long int block_off;
        
        char* block_ostr = strrchr(blocks[i], '_') + 1;

        sscanf(block_ostr, "%ld", &block_off);

        fprintf(stderr, "sea: block offset %ld\n", block_off);

        struct stat *tempst = malloc(sizeof(struct stat));
        lstat(blocks[i], tempst);

        if (size > tempst->st_size)
        {
            readsize = tempst->st_size;
            src->buf[i].pos = 0; //TODO: this is likely not always 0 if there are a bunch of tiny files
        }
        else
        {
            readsize = size;
            src->buf[i].pos = block_off;
        }


        src->buf[i].size = readsize;
        src->buf[i].flags = FUSE_BUF_IS_FD | FUSE_BUF_FD_SEEK;
        src->buf[i].fd = fd;
        //src->buf[i].mem = malloc(tempst->st_size);
        src->count++;
    }

	*bufp = src;

    fprintf(stderr, "Sea: read buff of path %s completed.\n", path);
	return 0;
}

static int sea_write(const char *path, const char *buf, size_t size,
		     off_t offset, struct fuse_file_info *fi)
{
    struct sea_filep *f = get_filep(fi);
    fprintf(stderr, "Sea: write path %s\n", f->main_path);
	int res = 0;

	(void) path;
	res = pwrite(fi->fh, buf, size, offset);
	if (res < 0)
		res = -errno;

    fprintf(stderr, "Sea: write path %s completed\n", path);
	return res;
}

static int sea_write_buf(const char *path, struct fuse_bufvec *buf,
		     off_t offset, struct fuse_file_info *fi)
{
    struct sea_filep *f = get_filep(fi);
    fprintf(stderr, "Sea: write buff to path %s\n", f->curr_path);

    struct fuse_bufvec *dst;
    dst = malloc(sizeof(struct fuse_bufvec) * (f->block_count + 1));
	*dst = FUSE_BUFVEC_INIT(fuse_buf_size(buf));

    struct statvfs st;
	(void) path;

    if (statvfs(f->curr_path, &st) != 0){
        fprintf(stderr, "Sea: sea_fullpath: error getting current file filesystem info %s info \n", f->curr_path);
        abort();
    }

    int i = f->block_count;
    int fd = f->fd;
    // can issue write to current fd
    if (st.f_favail == 0 || st.f_bavail == 0){
        char fpath[PATH_MAX];
        char block_path[PATH_MAX]; 
        get_block_name(block_path, f->main_path, offset);
        sea_fullpath(fpath, block_path);

        f->fd = open(fpath, O_APPEND | O_CREAT | O_WRONLY);
        f->block_count += 1;
        strcpy(f->curr_path, fpath);
        f->curr_offset = offset;

        i += 1;

    }
    fprintf(stderr, "sea:current space in file partition: %lu %lu\n", st.f_favail, st.f_bavail);
    dst->buf[0].flags = FUSE_BUF_IS_FD | FUSE_BUF_FD_SEEK;
    dst->buf[0].fd = f->fd;
    dst->buf[0].pos = offset - f->curr_offset;

    fprintf(stderr, "sea: current position %ld \n", dst->buf[0].pos);


    fprintf(stderr, "Sea: write buf to path %s completed.\n", f->curr_path);
	return fuse_buf_copy(dst, buf, FUSE_BUF_SPLICE_NONBLOCK);
}

static int sea_statfs(const char *path, struct statvfs *stbuf)
{
    fprintf(stderr, "sea: getting file stats: %s\n", path);
	int res = 0;
    char fpath[PATH_MAX];
    // sea_fullpath(fpath, path);
	// res = statvfs(fpath, stbuf);

    // Check if file is partitioned into blocks
    char blocks[MAX_BLOCKS][PATH_MAX];
    sea_getblocks(blocks, path);

    if (blocks[0] !='\0')
        res = statvfs(blocks[0], stbuf);
    else
    {
        sea_fullpath(fpath, path);
	    res = statvfs(fpath, stbuf);
    }

    if (res < 0)
        return -errno;

    fprintf(stderr, "sea: obtained file stats: %s\n", fpath);
	return res;
}

static int sea_flush(const char *path, struct fuse_file_info *fi)
{
    fprintf(stderr, "Sea: flush path %s\n", path);
	int res = 0;

	(void) path;
	/* This is called from every close on an open file, so call the
	   close on the underlying filesystem.	But since flush may be
	   called multiple times for an open file, this must not really
	   close the file.  This is important if used on a network
	   filesystem like NFS which flush the data/metadata on close() */

    struct sea_filep *f = get_filep(fi);
	res = close(dup(f->fd));
	if (res < 0)
		return -errno;

    fprintf(stderr, "Sea: flush path %s completed.\n", path);
	return res;
}

static int sea_release(const char *path, struct fuse_file_info *fi)
{
    struct sea_filep *f = get_filep(fi);
    fprintf(stderr, "Sea: release path %s\n", f->main_path);
	(void) path;
	close(f->fd);
    fprintf(stderr, "Sea: release path %s completed.\n", f->main_path);

	return 0;
}

static int sea_fsync(const char *path, int isdatasync,
		     struct fuse_file_info *fi)
{
    struct sea_filep *f = get_filep(fi);
    fprintf(stderr, "Sea: fsync path %s\n", f->main_path);
	int res = 0;
	(void) path;

#ifndef HAVE_FDATASYNC
	(void) isdatasync;
#else
	if (isdatasync)
		res = fdatasync(f->fd);
	else
#endif
		res = fsync(f->fd);
	if (res < 0)
		return -errno;

    fprintf(stderr, "Sea: fsync path %s completed\n", path);
	return res;
}

#ifdef HAVE_POSIX_FALLOCATE
static int sea_fallocate(const char *path, int mode,
			off_t offset, off_t length, struct fuse_file_info *fi)
{
    struct sea_filep *f = get_filep(fi);
    fprintf(stderr, "Sea: fallocate path %s\n", f->main_path);
	(void) path;

	if (mode)
		return -EOPNOTSUPP;

	return -posix_fallocate(f->fd, offset, length);
}
#endif

#ifdef HAVE_SETXATTR
/* xattr operations are optional and can safely be left unimplemented */
static int sea_setxattr(const char *path, const char *name, const char *value,
			size_t size, int flags)
{
    fprintf(stderr, "Sea: setxattr path %s\n", path);
    char fpath[PATH_MAX];
    sea_fullpath(fpath, path);
	int res = lsetxattr(fpath, name, value, size, flags);
	if (res < 0)
		return -errno;
    fprintf(stderr, "Sea: setxattr path %s completed.\n", path);
	return res;
}

static int sea_getxattr(const char *path, const char *name, char *value,
			size_t size)
{
    fprintf(stderr, "Sea: getxattr path %s\n", path);
    char fpath[PATH_MAX];
    sea_fullpath(fpath, path);
	int res = lgetxattr(fpath, name, value, size);
    fprintf(stderr, "Sea: getxattr path %s completed\n", path);
	if (res < 0)
		return -errno;
	return res;
}

static int sea_listxattr(const char *path, char *list, size_t size)
{
    fprintf(stderr, "Sea: listxattr path %s\n", path);
    char fpath[PATH_MAX];
    sea_fullpath(fpath, path);
	int res = llistxattr(fpath, list, size);
    fprintf(stderr, "Sea: listxattr path %s completed\n", path);
	if (res < 0)
		return -errno;
	return res;
}

static int sea_removexattr(const char *path, const char *name)
{
    fprintf(stderr, "Sea: removexattr path %s\n", path);
    char fpath[PATH_MAX];
    sea_fullpath(fpath, path);
	int res = lremovexattr(fpath, name);
    fprintf(stderr, "Sea: removexattr path %s completed.\n", path);
	if (res < 0)
		return -errno;
	return res;
}
#endif /* HAVE_SETXATTR */

#ifdef HAVE_LIBULOCKMGR
static int sea_lock(const char *path, struct fuse_file_info *fi, int cmd,
		    struct flock *lock)
{
    fprintf(stderr, "Sea: lock path %s\n", path);
	(void) path;

	return ulockmgr_op(fi->fh, cmd, lock, &fi->lock_owner,
			   sizeof(fi->lock_owner));
}
#endif

static int sea_flock(const char *path, struct fuse_file_info *fi, int op)
{
    struct sea_filep *f = get_filep(fi);
    fprintf(stderr, "Sea: flock path %s\n", f->main_path);
	int res;
	(void) path;

	res = flock(f->fd, op);
    fprintf(stderr, "Sea: flock path %s completed.\n", f->main_path);
	if (res < 0)
		return -errno;

	return res;
}

#ifdef HAVE_COPY_FILE_RANGE
static ssize_t sea_copy_file_range(const char *path_in,
				   struct fuse_file_info *fi_in,
				   off_t off_in, const char *path_out,
				   struct fuse_file_info *fi_out,
				   off_t off_out, size_t len, int flags)
{
    //struct sea_filep *f = get_filep(fi);
    fprintf(stderr, "Sea: copy file range from %s to %s\n", path_in, path_out);
	ssize_t res;
	(void) path_in;
	(void) path_out;

	res = copy_file_range(fi_in->fh, &off_in, fi_out->fh, &off_out, len,
			      flags);
    fprintf(stderr, "Sea: copy file range from %s to %s completed.\n", path_in, path_out);
	if (res < 0)
		return -errno;

	return res;
}
#endif

static struct fuse_operations sea_oper = {
	.init           = sea_init,
	.getattr	= sea_getattr,
	.access		= sea_access,
	.readlink	= sea_readlink,
	.opendir	= sea_opendir,
	.readdir	= sea_readdir,
	.releasedir	= sea_releasedir,
	.mknod		= sea_mknod,
	.mkdir		= sea_mkdir,
	.symlink	= sea_symlink,
	.unlink		= sea_unlink,
	.rmdir		= sea_rmdir,
	.rename		= sea_rename,
	.link		= sea_link,
	.chmod		= sea_chmod,
	.chown		= sea_chown,
	.truncate	= sea_truncate,
#ifdef HAVE_UTIMENSAT
	.utimens	= sea_utimens,
#endif
	.create		= sea_create,
	.open		= sea_open,
	.read		= sea_read,
	.read_buf	= sea_read_buf,
	.write		= sea_write,
	.write_buf	= sea_write_buf,
	.statfs		= sea_statfs,
	.flush		= sea_flush,
	.release	= sea_release,
	.fsync		= sea_fsync,
#ifdef HAVE_POSIX_FALLOCATE
	.fallocate	= sea_fallocate,
#endif
#ifdef HAVE_SETXATTR
	.setxattr	= sea_setxattr,
	.getxattr	= sea_getxattr,
	.listxattr	= sea_listxattr,
	.removexattr	= sea_removexattr,
#endif
#ifdef HAVE_LIBULOCKMGR
	.lock		= sea_lock,
#endif
	.flock		= sea_flock,
#ifdef HAVE_COPY_FILE_RANGE
	.copy_file_range = sea_copy_file_range,
#endif
};


static void show_help(const char *progname)
{
    printf("usage: %s [options] <shareddir> <mountpoint>\n\n", progname);
    printf("File-system specific options:\n"
           "   --hierarchy_file=<s>   Filesystem hierarchy file\n\n");
}

int main(int argc, char *argv[])
{
    int ret;

    if ((argc < 3) || (argv[argc-2][0] == '-') || (argv[argc-1][0] == '-'))
    {
        show_help(argv[0]);
        //return 1;
    }

    options.rootdir = realpath(argv[argc-2], NULL);
    options.hierarchy_file = strdup("");
    argv[argc-2] = argv[argc-1];
    argv[argc-1] = NULL;
    argc--;
    fprintf(stderr, "argv args %s %s\n", argv[argc-2], argv[ argc -1]);

    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

    options.hierarchy_file = NULL;

    if (fuse_opt_parse(&args, &options, option_spec, NULL) == -1)
        return 1;

    /* When --help is specified, first print our own file-system
	   specific help text, then signal fuse_main to show
	   additional help (by adding `--help` to the options again)
	   without usage: line (by setting argv[0] to the empty
	   string) */
	if (options.show_help) {
		show_help(argv[0]);
		assert(fuse_opt_add_arg(&args, "--help") == 0);
		args.argv[0][0] = '\0';
	}
    char full_path [PATH_MAX+1];
    options.hierarchy_file = realpath(options.hierarchy_file, full_path);

    if (options.hierarchy_file == NULL){
        perror("Error: provided hierarchy file does not exist");
        return -errno;
    };
    printf("Hierarchy file: %s\n", options.hierarchy_file);
    FILE* fhierarchy = fopen(options.hierarchy_file, "r");
    if (fhierarchy == NULL){
        fprintf(stderr, "fuse: error opening hierarchy file: %s\n", options.hierarchy_file);
        exit(1);
    }

	umask(0);
    fprintf(stdout, "starting system\n");
	ret = fuse_main(args.argc, args.argv, &sea_oper, NULL);
    fprintf(stdout, "system started \n");
    fuse_opt_free_args(&args);
    return ret;
}
