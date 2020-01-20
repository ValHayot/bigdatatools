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

static void sea_fullpath(char fpath[PATH_MAX], const char *path)
{
    strcpy(fpath, SEA_DATA->rootdir);
    strncat(fpath, path, PATH_MAX);
}

//////////////////////////////////////////////////////////
// FUSE filesystem functions
// Comments obtained from /usr/include/fuse/fuse.h
/////////////////////////////////////////////////////////


/** Get file attributes.
 *
 * Similar to stat().  The 'st_dev' and 'st_blksize' fields are
 * ignored.	 The 'st_ino' field is ignored except if the 'use_ino'
 * mount option is given.
 */
int sea_getattr(const char *path, struct stat *statbuf)
{
    int retstat;
    char fpath[PATH_MAX];

    sea_fullpath(fpath, path);

    retstat = lstat(fpath, statbuf);

    return retstat;
}


/** Read the target of a symbolic link
 *
 * The buffer should be filled with a null terminated string.  The
 * buffer size argument includes the space for the terminating
 * null character.	If the linkname is too long to fit in the
 * buffer, it should be truncated.	The return value should be 0
 * for success.
 */
int sea_readlink(const char *path, char *link, size_t size)
{
    int retstat;
    char fpath[PATH_MAX];

    sea_fullpath(fpath, path);

    retstat = readlink(fpath, link, size - 1);
    if (retstat >= 0){
        link[retstat] = '\0';
        retstat = 0;
    }

    return retstat;
}


/** Create a file node
 *
 * This is called for creation of all non-directory, non-symlink
 * nodes.  If the filesystem defines a create() method, then for
 * regular files that will be called instead.
 */
int sea_mknod(const char *path, mode_t mode, dev_t dev)
{
    int retstat = 0;
    char fpath[PATH_MAX];

    sea_fullpath(fpath, path);

    if (S_ISREG(mode)){
        retstat = open(fpath, O_CREAT | O_EXCL | O_WRONLY, mode);
    } else
        if (S_ISFIFO(mode))
            retstat = mkfifo(fpath, mode);
        else
            retstat = mknod(fpath, mode, dev);
}


/** Create a directory 
 *
 * Note that the mode argument may not have the type specification
 * bits set, i.e. S_ISDIR(mode) can be false.  To obtain the
 * correct directory type bits use  mode|S_IFDIR
 * */
int sea_mkdir(const char *path, mode_t mode)
{
    char fpath[PATH_MAX];

    sea_fullpath(fpath, path);

    return mkdir(fpath, mode);
}

/** Remove a file */
int sea_unlink(const char *path)
{
    char fpath[PATH_MAX];
    sea_fullpath(fpath, path);

    return unlink(fpath);
}

/** Remove a directory */
int sea_rmdir(const char *path)
{
    char fpath[PATH_MAX];
    sea_fullpath(fpath, path);

    return rmdir(fpath);
}

/** Create a symbolic link */
int sea_symlink(const char *path, const char *link)
{
    char flink[PATH_MAX];
    sea_fullpath(flink, link);

    return symlink(path, flink);
}

/** Rename a file */
int sea_rename(const char *path, const char *newpath)
{
    char fpath[PATH_MAX];
    char fnewpath[PATH_MAX];

    sea_fullpath(fpath, path);
    sea_fullpath(fnewpath, newpath);

    return rename(fpath, fnewpath);
}

/** Create a hard link to a file */
int sea_link(const char *path, const char *newpath)
{
    char fpath[PATH_MAX], fnewpath[PATH_MAX];
    sea_fullpath(fpath, path);
    sea_fullpath(fnewpath, newpath);

    return link(fpath, fnewpath);
}

/** Change the permission bits of a file */
int sea_chmod(const char *path, mode_t mode)
{
    char fpath[PATH_MAX];
    sea_fullpath(fpath, path);

    return chmod(fpath, mode);
}

/** Change the owner and group of a file */
int sea_chown(const char *path, uid_t uid, gid_t gid)
{
    char fpath[PATH_MAX];
    sea_fullpath(fpath, path);

    return chown(fpath, uid, gid);
}

/** Change the size of a file */
int sea_truncate(const char *path, off_t newsize)
{
    char fpath[PATH_MAX];
    sea_fullpath(fpath, path);

    return truncate(fpath, newsize);
}


/** Change the access and/or modification times of a file
 *
 * Deprecated, use utimens() instead.
 */
// note: remove since deprecated
int sea_utime(const char *path, struct utimbuf *ubuf)
{
    char fpath[PATH_MAX];
    sea_fullpath(fpath, path);

    return utime(fpath, ubuf);
}


/** File open operation
 *
 * No creation (O_CREAT, O_EXCL) and by default also no
 * truncation (O_TRUNC) flags will be passed to open(). If an
 * application specifies O_TRUNC, fuse first calls truncate()
 * and then open(). Only if 'atomic_o_trunc' has been
 * specified and kernel version is 2.6.24 or later, O_TRUNC is
 * passed on to open.
 *
 * Unless the 'default_permissions' mount option is given,
 * open should check if the operation is permitted for the
 * given flags. Optionally open may also return an arbitrary
 * filehandle in the fuse_file_info structure, which will be
 * passed to all file operations.
 *
 * Changed in version 2.2
 */
int sea_open(const char *path, struct fuse_file_info *fi)
{
    int retstat = 0;
    int fd;
    char fpath[PATH_MAX];

    sea_fullpath(fpath, path);
    fd = open(fpath, fi->flags);

    fi->fh = fd;
    return retstat;
}


/** Read data from an open file
 *
 * Read should return exactly the number of bytes requested except
 * on EOF or error, otherwise the rest of the data will be
 * substituted with zeroes.	 An exception to this is when the
 * 'direct_io' mount option is specified, in which case the return
 * value of the read system call will reflect the return value of
 * this operation.
 *
 * Changed in version 2.2
 */
int sea_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    int retstat = 0;

    retstat = pread(fi->fh, buf, size, offset);
}


/** Write data to an open file
 *
 * Write should return exactly the number of bytes requested
 * except on error.	 An exception to this is when the 'direct_io'
 * mount option is specified (see read operation).
 *
 * Changed in version 2.2
 */
int sea_write(const char *path, const char *buf, size_t size, off_t offset,
              struct fuse_file_info *fi)
{
    int retstat = 0;

    retstat = pwrite(fi->fh, buf, size, offset);
}


/** Get file system statistics
 *
 * The 'f_frsize', 'f_favail', 'f_fsid' and 'f_flag' fields are ignored
 *
 * Replaced 'struct statfs' parameter with 'struct statvfs' in
 * version 2.5
 */
int sea_statfs(const char *path, struct statvfs *statv)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    sea_fullpath(fpath, path);

    retstat = statvfs(fpath, statv);

    return retstat;
}


/** Possibly flush cached data
 *
 * BIG NOTE: This is not equivalent to fsync().  It's not a
 * request to sync dirty data.
 *
 * Flush is called on each close() of a file descriptor.  So if a
 * filesystem wants to return write errors in close() and the file
 * has cached dirty data, this is a good place to write back data
 * and return any errors.  Since many applications ignore close()
 * errors this is not always useful.
 *
 * NOTE: The flush() method may be called more than once for each
 * open().	This happens if more than one file descriptor refers
 * to an opened file due to dup(), dup2() or fork() calls.	It is
 * not possible to determine if a flush is final, so each flush
 * should be treated equally.  Multiple write-flush sequences are
 * relatively rare, so this shouldn't be a problem.
 *
 * Filesystems shouldn't assume that flush will always be called
 * after some writes, or that if will be called at all.
 *
 * Changed in version 2.2
 */
int sea_flush(const char *path, struct fuse_file_info *fi)
{
    return 0;
}

/** Release an open file
 *
 * Release is called when there are no more references to an open
 * file: all file descriptors are closed and all memory mappings
 * are unmapped.
 *
 * For every open() call there will be exactly one release() call
 * with the same flags and file descriptor.	 It is possible to
 * have a file opened more than once, in which case only the last
 * release will mean, that no more reads/writes will happen on the
 * file.  The return value of release is ignored.
 *
 * Changed in version 2.2
 */
int sea_release(const char *path, struct fuse_file_info *fi)
{
    return close(fi->fh);
}

/** Synchronize file contents
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data.
 *
 * Changed in version 2.2
 */
int sea_fsync(const char *path, int datasync, struct fuse_file_info *fi)
{
#ifdef HAVE_FDATASYNC
    if (datasync)
        return fdatasync(fi->fh);
    else
#endif
        return fsync(fsync(fi->fh));
}

#ifdef HAVE_SYS_XATTR_H


/** Set extended attributes */
int sea_setxattr(const char *path, const char *name, const char *value, size_t size, int flags)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    sea_fullpath(fpath, path);

    retstat = lsetxattr(fpath, name, value, size, flags);

    return retstat;
}

/** Get extended attributes */
int sea_getxattr(const char *path, const char *name, char *value, size_t size)
{
    int retstat = 0;
    char fpath[PATH_MAX];

    sea_fullpath(fpath, path);

    retstat = lgetxattr(fpath, name, value, size);

    return retstat;
}

/** List extended attributes */
int sea_listxattr(const char *path, char *list, size_t size)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    char *ptr;

    sea_fullpath(fpath, path);
    
    retstart = llistxattr(fpath, list, size);
    if (retstat >= 0){
        if (list != NULL)
            for (ptr = list; ptr < list + retstat; ptr += strlen(ptr)+1)
                fprintf("    \"%s\"\n", ptr);
        else
            fprintf("    (null)\n");
    }

    return retstat;
}

/** Remove extended attributes */
int sea_removexattr(const char *path, const char *name)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    sea_fullpath(fpath, path);
    
    retstat = lremovexattr(fpath, name);
    return retstat;
}
#endif

/** Open directory
 *
 * Unless the 'default_permissions' mount option is given,
 * this method should check if opendir is permitted for this
 * directory. Optionally opendir may also return an arbitrary
 * filehandle in the fuse_file_info structure, which will be
 * passed to readdir, releasedir and fsyncdir.
 *
 * Introduced in version 2.3
 */
int sea_opendir(const char *path, struct fuse_file_info *fi)
{
    DIR *dp;
    int retstat = 0;
    char fpath[PATH_MAX];

    sea_fullpath(fpath, path);

    dp = opendir(fpath);

    if (dp == NULL)
        retstat = -errno;

    fi->fh = (intptr_t) dp;

    return retstat;
}

/** Read directory
 *
 * This supersedes the old getdir() interface.  New applications
 * should use this.
 *
 * The filesystem may choose between two modes of operation:
 *
 * 1) The readdir implementation ignores the offset parameter, and
 * passes zero to the filler function's offset.  The filler
 * function will not return '1' (unless an error happens), so the
 * whole directory is read in a single readdir operation.  This
 * works just like the old getdir() method.
 *
 * 2) The readdir implementation keeps track of the offsets of the
 * directory entries.  It uses the offset parameter and always
 * passes non-zero offset to the filler function.  When the buffer
 * is full (or an error happens) the filler function will return
 * '1'.
 *
 * Introduced in version 2.3
 */
int sea_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
                struct fuse_file_info *fi){
    int retstat = 0;
    DIR *dp;
    struct dirent *de;

    dp = (DIR *) (uintptr_t) fi->fh;

    de = readdir(dp);

    if (de == 0) {
        retstat = -errno;
        return retstat;
    }

    do {
        if (filler(buf, de->d_name, NULL, 0) != 0) {
            return -ENOMEM;
        }
    } while ((de = readdir(dp)) != NULL);

    return retstat;
}

/** Release directory
 *
 * Introduced in version 2.3
 */
int sea_releasedir(const char *path, struct fuse_file_info *fi)
{
    int retstat = 0;
    closedir((DIR *) (uintptr_t) fi->fh);

    return retstat;
}

/** Synchronize directory contents
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data
 *
 * Introduced in version 2.3
 */
int sea_fsyncdir(const char *path, int datasync, struct fuse_file_info *fi)
{
    int retstat = 0;
    return retstat;
}

/**
 * Initialize filesystem
 *
 * The return value will passed in the private_data field of
 * fuse_context to all file operations and as a parameter to the
 * destroy() method.
 *
 * Introduced in version 2.3
 * Changed in version 2.6
 */
void *sea_init(struct fuse_conn_info *conn)
{
    return SEA_DATA;
}


/**
 * Clean up filesystem
 *
 * Called on filesystem exit.
 *
 * Introduced in version 2.3
 */
void sea_destroy(void *userdata){
    printf("Cleanup called\n");
}

/**
 * Check file access permissions
 *
 * This will be called for the access() system call.  If the
 * 'default_permissions' mount option is given, this method is not
 * called.
 *
 * This method is not called under Linux kernel versions 2.4.x
 *
 * Introduced in version 2.5
 */
int sea_access(const char *path, int mask)
{
    int retstat = 0;
    char fpath[PATH_MAX];

    sea_fullpath(fpath, path);

    retstat = access(fpath, mask);

    return retstat;
}

/**
 * Create and open a file
 *
 * If the file does not exist, first create it with the specified
 * mode, and then open it.
 *
 * If this method is not implemented or under Linux kernel
 * versions earlier than 2.6.15, the mknod() and open() methods
 * will be called instead.
 *
 * Introduced in version 2.5
 */
// Not implemented.  I had a version that used creat() to create and
// open the file, which it turned out opened the file write-only.


/**
 * Change the size of an open file
 *
 * This method is called instead of the truncate() method if the
 * truncation was invoked from an ftruncate() system call.
 *
 * If this method is not implemented or under Linux kernel
 * versions earlier than 2.6.15, the truncate() method will be
 * called instead.
 *
 * Introduced in version 2.5
 */
int sea_ftruncate(const char *path, off_t offset, struct fuse_file_info *fi)
{
    int retstat = 0;

    retstat = ftruncate(fi->fh, offset);

    return retstat;
}


/**
 * Get attributes from an open file
 *
 * This method is called instead of the getattr() method if the
 * file information is available.
 *
 * Currently this is only called after the create() method if that
 * is implemented (see above).  Later it may be called for
 * invocations of fstat() too.
 *
 * Introduced in version 2.5
 */
int sea_fgetattr(const char *path, struct stat *statbuf, struct fuse_file_info *fi)
{
    int retstat = 0;

    if (!strcmp(path, "/"))
        return sea_getattr(path, statbuf);

    retstat = fstat(fi->fh, statbuf);

    return retstat;
}


struct fuse_operations sea_oper = {
    .getattr = sea_getattr,
    .readlink = sea_readlink,
    .getdir = NULL, // deprecated
    .mknod = sea_mknod,
    .mkdir = sea_mkdir,
    .unlink = sea_unlink,
    .rmdir = sea_rmdir,
    .symlink = sea_symlink,
    .rename = sea_rename,
    .link = sea_link,
    .chmod = sea_chmod,
    .chown = sea_chown,
    .truncate = sea_truncate,
    .utime = sea_utime,
    .open = sea_open,
    .read = sea_read,
    .write = sea_write,
    .statfs = sea_statfs,
    .flush = sea_flush,
    .release = sea_release,
    .fsync = sea_fsync,

#ifdef HAVE_SYS_XATTR_H
    .setxattr = sea_setxattr,
    .getxattr = sea_getxattr,
    .listxattr = sea_listxattr,
    .removexattr = sea_removexattr,
#endif

    .opendir = sea_opendir,
    .readdir = sea_readdir,
    .releasedir = sea_releasedir,
    .fsyncdir = sea_fsyncdir,
    .init = sea_init,
    .destroy = sea_destroy,
    .access = sea_access,
    .ftruncate = sea_ftruncate,
    .fgetattr = sea_fgetattr
};

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

    sea_data->rootdir = realpath(argv[argc-2], NULL);
    argv[argc-2] = argv[argc-1] = NULL;
    argc--;

    fprintf(stderr, "about to call fuse main\n");
    fuse_stat = fuse_main(argc, argv, &sea_oper, sea_data);
    fprintf(stderr, "fuse_main returned %d\n", fuse_stat);

}
