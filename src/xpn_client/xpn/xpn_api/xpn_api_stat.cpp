
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

#include "base_cpp/fixed_string.hpp"
#include "base_cpp/fixed_task_queue.hpp"
#include "xpn/xpn_api.hpp"
#include "base_cpp/xpn_path.hpp"

namespace XPN
{
    struct format_stat {
        struct ::stat &st;
        friend std::ostream& operator<<(std::ostream& os, const format_stat& st) {
            os << "--- File Status (struct stat) ---\n";

            os << std::left << std::setw(18) << "Device ID:"      << st.st.st_dev << "\n";
            os << std::left << std::setw(18) << "Inode Number:"   << st.st.st_ino << "\n";
            
            os << std::left << std::setw(18) << "File Type:";
            if (S_ISREG(st.st.st_mode))        os << "Regular file\n";
            else if (S_ISDIR(st.st.st_mode))   os << "Directory\n";
            else if (S_ISLNK(st.st.st_mode))   os << "Symbolic link\n";
            else if (S_ISCHR(st.st.st_mode))   os << "Character device\n";
            else if (S_ISBLK(st.st.st_mode))   os << "Block device\n";
            else if (S_ISFIFO(st.st.st_mode))  os << "FIFO (Named pipe)\n";
            else if (S_ISSOCK(st.st.st_mode))  os << "Socket\n";
            else                               os << "Unknown\n";
            
            os << std::left << std::setw(18) << "Permissions:"   << format_open_mode(st.st.st_mode) << "\n";
            os << std::left << std::setw(18) << "Hard Links:"     << st.st.st_nlink << "\n";
            
            os << std::left << std::setw(18) << "User ID (UID):"  << st.st.st_uid << "\n";
            os << std::left << std::setw(18) << "Group ID (GID):" << st.st.st_gid << "\n";
            
            if (S_ISCHR(st.st.st_mode) || S_ISBLK(st.st.st_mode)) {
                os << std::left << std::setw(18) << "Device Type (Rdev):" << st.st.st_rdev << "\n";
            }
            
            os << std::left << std::setw(18) << "Total Size:"     << st.st.st_size << " bytes\n";
            os << std::left << std::setw(18) << "Block Size:"     << st.st.st_blksize << " bytes\n";
            os << std::left << std::setw(18) << "Blocks Allocated:" << st.st.st_blocks << "\n";
            
            std::time_t atime = st.st.st_atime;
            std::time_t mtime = st.st.st_mtime;
            std::time_t ctime = st.st.st_ctime;
            
            os << std::left << std::setw(18) << "Last Access:"    << std::ctime(&atime);
            os << std::left << std::setw(18) << "Last Modify:"    << std::ctime(&mtime);
            os << std::left << std::setw(18) << "Status Change:"   << std::ctime(&ctime);
            os << "---------------------------------";
            return os;
        }
    };

    int xpn_api::internal_stat(xpn_file &file, struct ::stat *sb) {
        XPN_DEBUG_BEGIN_CUSTOM(file.m_path);
        int res = 0;

        if (read_metadata(file.m_mdata) < 0){
            res = -1;
            XPN_DEBUG_END_CUSTOM(file.m_path);
            return res;
        }
        
        int master = file.m_mdata.master_file();
        if (master < 0) {
            res = master;
            XPN_DEBUG_END_CUSTOM(file.m_path);
            return res;
        }

        while (master >= 0) {
            res = file.m_part.m_data_serv[master]->nfi_getattr(file.m_path, *sb);
            XPN_DEBUG("Stat from serv "<<master<<" res "<<res);
            if (res < 0 && file.m_part.m_data_serv[master]->m_error < 0) {
                master = file.m_mdata.master_file();
                XPN_DEBUG("Retry stat in "<<master);
                continue;
            }
            break;
        }

        // Update file_size
        if (S_ISREG(sb->st_mode)){
            sb->st_size = file.m_mdata.m_data.file_size;
        }

        XPN_DEBUG_END_CUSTOM(file.m_path<<"\n"<<format_stat(*sb));
        return res;
    }

    int xpn_api::fstat(int fd, struct ::stat *sb)
    {
        XPN_DEBUG_BEGIN_CUSTOM(fd);
        int res = 0;

        auto file = m_file_table.get(fd);
        if (!file)
        {
            errno = EBADF;
            XPN_DEBUG_END_CUSTOM(fd);
            return -1;
        }

        res = internal_stat(*file, sb);

        XPN_DEBUG_END_CUSTOM(fd);
        return res;
    }

    int xpn_api::stat(const char *path, struct ::stat *sb)
    {
        XPN_DEBUG_BEGIN_CUSTOM(path);
        int res = 0;
        FixedStringPath file_path;
        auto name_part = check_remove_part_from_path(path, file_path);
        if (name_part.empty()) {
            errno = ENOENT;
            XPN_DEBUG_END_CUSTOM(path);
            return -1;
        }

        xpn_file file(file_path, m_partitions.find(name_part)->second);

        res = internal_stat(file, sb);

        XPN_DEBUG_END_CUSTOM(path);
        return res;
    }

    int xpn_api::chown([[maybe_unused]] const char *path, [[maybe_unused]] uid_t owner, [[maybe_unused]] gid_t group)
    {
        XPN_DEBUG_BEGIN;
        int res = 0;
        // TODO
        XPN_DEBUG_END;
        return res;
    }

    int xpn_api::fchown([[maybe_unused]] int fd, [[maybe_unused]] uid_t owner, [[maybe_unused]] gid_t group)
    {
        XPN_DEBUG_BEGIN;
        int res = 0;
        // TODO
        XPN_DEBUG_END;
        return res;
    }

    int xpn_api::chmod([[maybe_unused]] const char *path, [[maybe_unused]] mode_t mode)
    {
        XPN_DEBUG_BEGIN;
        int res = 0;
        // TODO
        XPN_DEBUG_END;
        return res;
    }

    int xpn_api::fchmod([[maybe_unused]] int fd, [[maybe_unused]] mode_t mode)
    {
        XPN_DEBUG_BEGIN;
        int res = 0;
        // TODO
        XPN_DEBUG_END;
        return res;
    }

    int xpn_api::truncate([[maybe_unused]] const char *path, [[maybe_unused]] int64_t length)
    {
        XPN_DEBUG_BEGIN;
        int res = 0;
        // TODO
        XPN_DEBUG_END;
        return res;
    }

    int xpn_api::ftruncate([[maybe_unused]] int fd, [[maybe_unused]] int64_t length)
    {
        XPN_DEBUG_BEGIN;
        int res = 0;
        // TODO
        XPN_DEBUG_END;
        return res;
    }

    int xpn_api::internal_statvfs(xpn_file& file, struct ::statvfs *buf) {
        XPN_DEBUG_BEGIN_CUSTOM(file.m_path);
        int res = 0;

        int master_file = file.m_mdata.master_file();
        if (master_file < 0) {
            XPN_DEBUG_END_CUSTOM(file.m_path);
            return master_file;
        }
        auto& server = file.m_part.m_data_serv[master_file];
        
        res = server->nfi_statvfs(file.m_path, *buf);

        if (res < 0){
            XPN_DEBUG_END_CUSTOM(file.m_path);
            return res;
        }

        auto result_handler = [&](const WorkerResult& r) {
            if (r.result < 0) {
                res = r.result;
                errno = r.errorno;
            }
            return true; // Continue
        };
        FixedTaskQueue tasks(*m_worker, result_handler);
        std::mutex buff_mutex;
        for (uint64_t i = 0; i < file.m_part.m_data_serv.size(); i++)
        {
            if (static_cast<int>(i) == master_file) continue;
            if (file.m_part.m_data_serv[i]->m_error < 0) continue;
            tasks.launch([i, &file, &buf, &buff_mutex](){
                struct ::statvfs aux_buf;
                int res = file.m_part.m_data_serv[i]->nfi_statvfs(file.m_path, aux_buf);
                std::unique_lock lock(buff_mutex);
                if (res >= 0){
                    buf->f_blocks += aux_buf.f_blocks;
                    buf->f_bfree += aux_buf.f_bfree;
                    buf->f_bavail += aux_buf.f_bavail;
                    
                    buf->f_files += aux_buf.f_files;
                    buf->f_ffree += aux_buf.f_ffree;
                    buf->f_favail += aux_buf.f_favail;
                }
                return WorkerResult(res);
            });
        }

        tasks.wait_remaining();

        XPN_DEBUG_END_CUSTOM(file.m_path);
        return res;
    }

    int xpn_api::statvfs(const char * path, struct ::statvfs *buf)
    {
        XPN_DEBUG_BEGIN_CUSTOM(path);
        int res = 0;

        FixedStringPath file_path;
        auto name_part = check_remove_part_from_path(path, file_path);
        if (name_part.empty()){
            errno = ENOENT;
            res = -1;
            XPN_DEBUG_END_CUSTOM(path);
            return res;
        }

        xpn_file file(file_path, m_partitions.find(name_part)->second);

        res = internal_statvfs(file, buf);

        XPN_DEBUG_END_CUSTOM(path);
        return res;
    }

    int xpn_api::fstatvfs(int fd, struct ::statvfs *buf)
    {
        XPN_DEBUG_BEGIN;
        int res = 0;

        auto file = m_file_table.get(fd);
        if (!file)
        {
            errno = EBADF;
            XPN_DEBUG_END_CUSTOM(fd);
            return -1;
        }

        res = internal_statvfs(*file, buf);

        XPN_DEBUG_END;
        return res;
    }
} // namespace XPN
