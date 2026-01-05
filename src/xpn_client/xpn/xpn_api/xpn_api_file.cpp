
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

#include "base_cpp/debug.hpp"
#include "base_cpp/fixed_task_queue.hpp"
#include "xpn/xpn_api.hpp"

#include <fcntl.h>

#include <iostream>

namespace XPN
{
    std::string_view xpn_api::check_remove_part_from_path(std::string_view path, FixedStringPath& out_path)
    {
        std::string_view name_part = xpn_path::get_first_dir(path);
        // XPN_DEBUG("First dir "<<name_part);
        auto it = m_partitions.find(name_part);
        if (it == m_partitions.end()) {
            return {};
        }

        auto first_dir = xpn_path::remove_first_dir(path);
        out_path.clear();

        if (!first_dir.empty() && first_dir[0] != '/') {
            out_path.append(cwd);
            out_path.append("/");
            out_path.append(first_dir);
        } else {
            out_path.append(first_dir);
        }

        return name_part;
    }

    int xpn_api::open(const char *path, int flags, mode_t mode)
    {
        XPN_DEBUG_BEGIN_CUSTOM(path<<", "<<format_open_flags(flags)<<", "<<format_open_mode(mode));
        int res = 0;

        FixedStringPath file_path;
        auto part_name = check_remove_part_from_path(path, file_path);
        if (part_name.empty()){
            errno = ENOENT;
            XPN_DEBUG_END_CUSTOM(path<<", "<<format_open_flags(flags)<<", "<<format_open_mode(mode));
            return -1;
        }

        std::shared_ptr<xpn_file> file = std::make_shared<xpn_file>(file_path, m_partitions.find(part_name)->second);

        if ((O_DIRECTORY != (flags & O_DIRECTORY))) {
            res = read_metadata(file->m_mdata);
            if (res < 0 && O_CREAT != (flags & O_CREAT)){
                XPN_DEBUG_END_CUSTOM(path<<", "<<format_open_flags(flags)<<", "<<format_open_mode(mode));
                return -1;
            }

            if(!file->m_mdata.m_data.is_valid()){
                file->m_mdata.m_data.fill(file->m_mdata);
            }
        }

        if ((O_CREAT == (flags & O_CREAT))) {
            auto result_handler = [&](const WorkerResult& r) {
                if (r.result < 0) {
                    res = r.result;
                    errno = r.errorno;
                    return false; // Stop
                }
                return true; // Continue
            };
            FixedTaskQueue tasks(*m_worker, result_handler);
            for (uint64_t i = 0; i < file->m_part.m_data_serv.size(); i++) {
                if (!file->exist_in_serv(i)) continue;

                auto& serv = file->m_part.m_data_serv[i];
                bool ok = tasks.launch([i, &serv, &file, flags, mode](){
                        int res = serv->nfi_open(file->m_path, flags, mode, file->m_data_vfh[i]);
                        return WorkerResult(res);
                });
                if (!ok) {
                    XPN_DEBUG_END_CUSTOM(path<<", "<<format_open_flags(flags)<<", "<<format_open_mode(mode));
                    return res;
                }
            }

            if (!tasks.wait_remaining()) {
                XPN_DEBUG_END_CUSTOM(path<<", "<<format_open_flags(flags)<<", "<<format_open_mode(mode));
                return res;
            }

            if ((O_DIRECTORY != (flags & O_DIRECTORY))) {
                write_metadata(file->m_mdata, false);
            }
        }else{
            int master_file = file->m_mdata.master_file();
            if ((O_DIRECTORY == (flags & O_DIRECTORY))){
                res = file->m_part.m_data_serv[master_file]->nfi_opendir(file->m_path, file->m_data_vfh[master_file]);
            }else{
                res = file->m_part.m_data_serv[master_file]->nfi_open(file->m_path, flags, mode, file->m_data_vfh[master_file]);
            }
            if (res < 0) {
                XPN_DEBUG_END_CUSTOM(path<<", "<<format_open_flags(flags)<<", "<<format_open_mode(mode));
                return res;
            }
        }

        
        if ((O_DIRECTORY == (flags & O_DIRECTORY))){
            file->m_type = file_type::dir;
        }else{
            file->m_type = file_type::file;
        }
        file->m_flags = flags;
        file->m_mode = mode;
        res = m_file_table.insert(file);

        XPN_DEBUG_END_CUSTOM(path<<", "<<format_open_flags(flags)<<", "<<format_open_mode(mode));
        return res;
    }

    int xpn_api::creat(const char *path, mode_t perm)
    {
        XPN_DEBUG_BEGIN_CUSTOM(path<<", "<<perm);
        // Like in unix we use the open with flags
        int res = open(path, O_WRONLY|O_CREAT|O_TRUNC, perm);
        XPN_DEBUG_END_CUSTOM(path<<", "<<perm);
        return res;
    }

    int xpn_api::close(int fd)
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

        if (file->m_links == 0) {
            auto result_handler = [&](const WorkerResult& r) {
                if (r.result < 0) {
                    res = r.result;
                    errno = r.errorno;
                }
                return true; // Continue
            };
            FixedTaskQueue tasks(*m_worker, result_handler);
            for (uint64_t i = 0; i < file->m_data_vfh.size(); i++) {
                if (file->m_data_vfh[i].is_file() && file->m_data_vfh[i].as_file().fd != -1) {
                    tasks.launch([i, &file]() { 
                        int res = file->m_part.m_data_serv[i]->nfi_close(file->m_path, file->m_data_vfh[i]); 
                        return WorkerResult(res);
                    });
                }
            }

            tasks.wait_remaining();
        } else {
            file->m_links--;
        }

        m_file_table.remove(fd);

        XPN_DEBUG_END_CUSTOM(fd);
        return res;
    }

    int xpn_api::unlink(const char *path)
    {
        XPN_DEBUG_BEGIN_CUSTOM(path);
        int res = 0;

        FixedStringPath file_path;
        auto part_name = check_remove_part_from_path(path, file_path);
        if (part_name.empty()){
            errno = ENOENT;
            XPN_DEBUG_END_CUSTOM(path);
            return -1;
        }

        xpn_file file(file_path, m_partitions.find(part_name)->second);

        res = read_metadata(file.m_mdata);
        if (res < 0){
            XPN_DEBUG_END_CUSTOM(path);
            return res;
        }
        res = 0;
        
        auto result_handler = [&](const WorkerResult& r) {
            if (r.result < 0) {
                res = r.result;
                errno = r.errorno;
            }
            return true; // Continue
        };
        FixedTaskQueue tasks(*m_worker, result_handler);
        for (uint64_t i = 0; i < file.m_part.m_data_serv.size(); i++)
        {
            if (file.exist_in_serv(i)){
                tasks.launch([i, &file](){
                    // Always wait and not async because it can fail in other ways
                    int res = file.m_part.m_data_serv[i]->nfi_remove(file.m_path, false);
                    return WorkerResult(res);
                    // v_res[i] = file.m_part.m_data_serv[i]->nfi_remove(file.m_path, file.m_mdata.master_file()==static_cast<int>(i));
                });
            }
        }

        tasks.wait_remaining();

        XPN_DEBUG_END_CUSTOM(path);
        return res;
    }

    int xpn_api::rename(const char *path, const char *newpath)
    {
        XPN_DEBUG_BEGIN_CUSTOM(path<<", "<<newpath);
        int res = 0;
        FixedStringPath file_path;
        auto part_name = check_remove_part_from_path(path, file_path);
        if (part_name.empty()){
            errno = ENOENT;
            XPN_DEBUG_END_CUSTOM(path<<", "<<newpath);
            return -1;
        }
        FixedStringPath new_file_path;
        auto new_part_name = check_remove_part_from_path(newpath, new_file_path);
        if (new_part_name.empty()){
            errno = ENOENT;
            XPN_DEBUG_END_CUSTOM(path<<", "<<newpath);
            return -1;
        }

        if(part_name != new_part_name){
            XPN_DEBUG_END_CUSTOM(path<<", "<<newpath);
            return -1;
        }

        xpn_file file(file_path, m_partitions.find(part_name)->second);
        xpn_file new_file(new_file_path, m_partitions.find(new_part_name)->second);

        res = read_metadata(file.m_mdata);
        if (res < 0){
            XPN_DEBUG_END_CUSTOM(path<<", "<<newpath);
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
        for (uint64_t i = 0; i < file.m_part.m_data_serv.size(); i++)
        {
            if (file.exist_in_serv(i)){
                if (!new_file.exist_in_serv(i)){
                    XPN_DEBUG("Remove in server "<<i);
                    tasks.launch([i, &file](){
                        int res = file.m_part.m_data_serv[i]->nfi_remove(file.m_path, false);
                        return WorkerResult(res);
                    });
                }else{
                    XPN_DEBUG("Rename in server "<<i);
                    tasks.launch([i, &file, &new_file](){
                        int res = file.m_part.m_data_serv[i]->nfi_rename(file.m_path, new_file.m_path);
                        return WorkerResult(res);
                    });
                }
            }
        }

        tasks.wait_remaining();

        if (res >= 0){
            new_file.m_mdata.m_data = file.m_mdata.m_data;
            res = write_metadata(new_file.m_mdata, false);
        }

        XPN_DEBUG_END_CUSTOM(path<<", "<<newpath);
        return res;
    }
    
    int xpn_api::transfer_xpn2fs(const char *path, const char *newpath)
    {
        XPN_DEBUG_BEGIN_CUSTOM(path<<", "<<newpath);
        int res = 0;

        int fd1 = open(path, O_RDONLY, 0);
        if (fd1 < 0){
            res = fd1;
            XPN_DEBUG_END_CUSTOM(path<<", "<<newpath);
            return res;
        }
        struct stat st = {};
        if (stat(path, &st) < 0) {
            res = -1;
            XPN_DEBUG_END_CUSTOM(path<<", "<<newpath);
            return res;
        }
        
        int fd2 = ::open(newpath, O_WRONLY | O_CREAT | O_TRUNC, st.st_mode);
        if (fd2 < 0){
            res = fd2;
            XPN_DEBUG_END_CUSTOM(path<<", "<<newpath);
            return res;
        }

        int64_t file_size = st.st_size;
        int64_t current_offset = 0;
        std::vector<uint8_t> buffer(MAX_BUFFER_SIZE, 0);
        while (file_size > 0) {
            int64_t read_size = pread(fd1, buffer.data(), buffer.size(), current_offset);
            if (read_size < 0) {
                res = -1;
                XPN_DEBUG_END_CUSTOM(path<<", "<<newpath);
                return res;
            }
            int64_t write_size = filesystem::pwrite(fd2, buffer.data(), read_size, current_offset);
            if (read_size < 0) {
                res = -1;
                XPN_DEBUG_END_CUSTOM(path<<", "<<newpath);
                return res;
            }
            file_size -= write_size;
            current_offset -= write_size;
        }

        int ret0 = unlink(path);
        int ret1 = close(fd1);
        int ret2 = ::close(fd2);
        if (ret0 < 0 || ret1 < 0 || ret2 < 0) {
            res = -1;
            XPN_DEBUG_END_CUSTOM(path<<", "<<newpath);
            return res;
        }

        XPN_DEBUG_END_CUSTOM(path<<", "<<newpath);
        return res;
    }

    int xpn_api::transfer_fs2xpn(const char *path, const char *newpath)
    {
        XPN_DEBUG_BEGIN_CUSTOM(path<<", "<<newpath);
        int res = 0;

        int fd1 = ::open(path, O_RDONLY, 0);
        if (fd1 < 0){
            res = fd1;
            XPN_DEBUG_END_CUSTOM(path<<", "<<newpath);
            return res;
        }
        struct stat st = {};
        if (::stat(path, &st) < 0) {
            res = -1;
            XPN_DEBUG_END_CUSTOM(path<<", "<<newpath);
            return res;
        }
        
        int fd2 = open(newpath, O_WRONLY | O_CREAT | O_TRUNC, st.st_mode);
        if (fd2 < 0){
            res = fd2;
            XPN_DEBUG_END_CUSTOM(path<<", "<<newpath);
            return res;
        }

        int64_t file_size = st.st_size;
        int64_t current_offset = 0;
        std::vector<uint8_t> buffer(MAX_BUFFER_SIZE, 0);
        while (file_size > 0) {
            int64_t read_size = filesystem::pread(fd1, buffer.data(), buffer.size(), current_offset);
            if (read_size < 0) {
                res = -1;
                XPN_DEBUG_END_CUSTOM(path<<", "<<newpath);
                return res;
            }
            int64_t write_size = pwrite(fd2, buffer.data(), read_size, current_offset);
            if (read_size < 0) {
                res = -1;
                XPN_DEBUG_END_CUSTOM(path<<", "<<newpath);
                return res;
            }
            file_size -= write_size;
            current_offset -= write_size;
        }

        int ret0 = ::unlink(path);
        int ret1 = ::close(fd1);
        int ret2 = close(fd2);
        if (ret0 < 0 || ret1 < 0 || ret2 < 0) {
            res = -1;
            XPN_DEBUG_END_CUSTOM(path<<", "<<newpath);
            return res;
        }

        XPN_DEBUG_END_CUSTOM(path<<", "<<newpath);
        return res;
    }

    int xpn_api::dup(int fd)
    {
        XPN_DEBUG_BEGIN_CUSTOM(fd);
        int res = 0;
        res = m_file_table.dup(fd);
        if (res < 0){
            errno = EBADF;
        }
        XPN_DEBUG_END_CUSTOM(fd);
        return res;
    }

    int xpn_api::dup2(int fd, int fd2)
    {
        XPN_DEBUG_BEGIN_CUSTOM(fd<<", "<<fd2);
        int res = 0;
        res = m_file_table.dup(fd, fd2);
        if (res < 0){
            errno = EBADF;
        }
        XPN_DEBUG_END_CUSTOM(fd<<", "<<fd2);
        return res;
    }

} // namespace XPN
