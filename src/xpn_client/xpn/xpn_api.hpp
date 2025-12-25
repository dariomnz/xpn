
/*
 *  Copyright 2020-2024 Felix Garcia Carballeira, Diego Camarmas Alonso, Alejandro Calderon Mateos, Dario Muñoz Muñoz
 *
 *  This file is part of Expand.
 *
 *  Expand is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Lesser General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Expand is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with Expand.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

#pragma once

#include <string>
#include <unordered_map>
#include <mutex>
#include <sys/stat.h>
#include <sys/statvfs.h>

#include "base_cpp/fixed_string.hpp"
#include "base_cpp/str_unordered_map.hpp"
#include "xpn/xpn_partition.hpp"
#include "xpn/xpn_file_table.hpp"
#include "xpn/xpn_rw.hpp"
#include "base_cpp/debug.hpp"
#include "base_cpp/workers.hpp"

#ifdef _REENTRANT

    #define XPN_API_LOCK()    std::unique_lock<std::mutex> lock(XPN::xpn_api::get_instance().m_api_mutex);
    #define XPN_API_UNLOCK()  lock.unlock();

#else

    #define XPN_API_LOCK()    
    #define XPN_API_UNLOCK()  

#endif

#ifndef O_RDONLY
#define O_RDONLY             00
#endif
#ifndef O_WRONLY
#define O_WRONLY             01
#endif
#ifndef O_RDWR
#define O_RDWR               02
#endif
#ifndef O_CREAT
#define O_CREAT            0100
#endif
#ifndef O_TRUNC
#define O_TRUNC           01000
#endif    
#ifndef O_APPEND
#define O_APPEND          02000
#endif
#ifndef O_DIRECTORY
#define O_DIRECTORY    00200000
#endif
namespace XPN
{
    class xpn_api
    {
    public:
        xpn_api() { init(); }
        ~xpn_api() { destroy(); }
        // Delete copy constructor
        xpn_api(const xpn_api&) = delete;
        // Delete copy assignment operator
        xpn_api& operator=(const xpn_api&) = delete;
        // Delete move constructor
        xpn_api(xpn_api&&) = delete;
        // Delete move assignment operator
        xpn_api& operator=(xpn_api&&) = delete;
        std::string_view check_remove_part_from_path(std::string_view path, FixedStringPath &out_path);
    public:
        static xpn_api& get_instance()
        {
            static xpn_api instance;
            return instance;
        }
    private:
        str_unordered_map<std::string, xpn_partition> m_partitions;
        xpn_file_table m_file_table;

        std::mutex m_init_mutex;
        bool m_initialized = false;

        std::string cwd;

    public:
        std::mutex m_api_mutex;
        std::unique_ptr<workers> m_worker;

    public:
        // XPN api
        int initialized();
        int init();
        int destroy();
        int print_partitions();
        int clean_connections();

        int flush_preload(const char * path, bool isFlush);
        int checkpoint(const char * path);

        int mark_error_server(int index);
        int get_block_locality(char *path, int64_t offset, int *url_c, char **url_v[]);
        int free_block_locality(int *url_c, char **url_v[]);

        // Metadata api
        int read_metadata(xpn_metadata &mdata);
        int write_metadata(xpn_metadata &mdata, bool only_file_size);

        // File api
        int   open      (const char *path, int flags, mode_t mode);
        int   creat     (const char *path, mode_t perm);
        int   close     (int fd);
        int   unlink    (const char *path);
        int   rename    (const char *path, const char *newpath);
        int   transfer_xpn2fs    (const char *path, const char *newpath);
        int   transfer_fs2xpn    (const char *path, const char *newpath);
        int   dup       (int fd);
        int   dup2      (int fd, int fd2);

        // Stat api
        int   internal_stat(xpn_file& file, struct ::stat *sb);
        int   fstat     (int fd, struct ::stat *sb);
        int   stat      (const char *path, struct ::stat *sb);
        int   chown     (const char *path,  uid_t owner,  gid_t group);
        int   fchown    (int  fd,  uid_t owner,  gid_t group);
        int   chmod     (const char *path,  mode_t mode);
        int   fchmod    (int fd,  mode_t mode);
        int   truncate  (const char *path,  int64_t length);
        int   ftruncate (int fd, int64_t length);
        int   internal_statvfs(xpn_file& file, struct ::statvfs *buf);
        int   statvfs   (const char *path, struct ::statvfs *buf);
        int   fstatvfs  (int fd, struct ::statvfs *buf);

        // RW api
        int64_t read            (int fd, void *buffer, uint64_t size);
        int64_t pread           (int fd, void *buffer, uint64_t size, int64_t offset);
        int64_t pread           (xpn_file& file, void *buffer, uint64_t size, int64_t offset);
        int64_t write           (int fd, const void *buffer, uint64_t size);
        int64_t pwrite          (int fd, const void *buffer, uint64_t size, int64_t offset);
        int64_t pwrite          (xpn_file& file, const void *buffer, uint64_t size, int64_t offset);
        int64_t   lseek           (int fd, int64_t offset, int flag);

        // f_file api
        FILE   *fopen   (const char *filename, const char *mode);
        int     fclose  (FILE *stream);
        uint64_t  fread   (void *ptr, uint64_t size, uint64_t nmemb, FILE *stream);
        uint64_t  fwrite  (const void *ptr, uint64_t size, uint64_t nmemb, FILE *stream);
        int     fseek   (FILE *stream, long offset, int whence);
        long    ftell   (FILE *stream);
        int     fflush  (FILE *stream);
        int     fgetc   (FILE *stream);
        char   *fgets   (char *s, int tam, FILE *stream);
        int     getc    (FILE *stream);
        void    rewind  (FILE *stream);
        int     fileno  (FILE *stream);
        int     ferror  (FILE *stream);

        // DIR api
        DIR *           opendir(const char *path);
        int             closedir(DIR *dirp);
        struct:: dirent*readdir(DIR *dirp);
        void            rewinddir(DIR *dirp);  
        int             mkdir(const char *path, mode_t perm);
        int             rmdir(const char *path);
        
        // cwd api
        char* getcwd(char *path, uint64_t size);
        int   chdir(char *path);

        // malleability
        enum class malleability_type {
            EXPAND,
            SHRINK
        };
        int start_malleability(const char* host_list, int rank, malleability_type type);
        int end_malleability(const char* host_list, int rank, malleability_type type);
    };
} // namespace XPN