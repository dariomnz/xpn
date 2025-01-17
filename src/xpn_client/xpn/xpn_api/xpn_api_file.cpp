
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
#include "xpn/xpn_api.hpp"

#include <iostream>

namespace XPN
{
    std::string xpn_api::check_remove_part_from_path(const std::string &path, std::string& out_path)
    {
        std::string name_part = xpn_path::get_first_dir(path);
        // XPN_DEBUG("First dir "<<name_part);
        auto it = m_partitions.find(name_part);
        if (it == m_partitions.end())
        {
            return {};
        }

        out_path = xpn_path::remove_first_dir(path);

        if (out_path[0] != '/'){
            out_path = cwd + '/' + out_path;
        }

        return name_part;
    }

    int xpn_api::open(const char *path, int flags, mode_t mode)
    {
        XPN_DEBUG_BEGIN_CUSTOM(path<<", "<<flags<<", "<<mode);
        int res = 0;

        std::string file_path;
        auto part_name = check_remove_part_from_path(path, file_path);
        if (part_name.empty()){
            errno = ENOENT;
            XPN_DEBUG_END_CUSTOM(path<<", "<<flags<<", "<<mode);
            return -1;
        }

        std::shared_ptr<xpn_file> file = std::make_shared<xpn_file>(file_path, m_partitions.at(part_name));
        #ifndef USE_ROCKSDB
        if ((O_DIRECTORY != (flags & O_DIRECTORY)))
        {
        #endif
            res = read_metadata(file->m_mdata);
            if (res < 0 && O_CREAT != (flags & O_CREAT)){
                XPN_DEBUG_END_CUSTOM(path<<", "<<flags<<", "<<mode);
                return -1;
            }

            if(!file->m_mdata.m_data.is_valid()){
                file->m_mdata.m_data.fill(file->m_mdata);
            }
        #ifndef USE_ROCKSDB
        }
        #endif

        if ((O_CREAT == (flags & O_CREAT))){

            std::vector<std::future<int>> v_res(file->m_part.m_data_serv.size());
            for (size_t i = 0; i < file->m_part.m_data_serv.size(); i++)
            {
                auto& serv = file->m_part.m_data_serv[i];
                if (file->exist_in_serv(i)){
                    v_res[i] = m_worker->launch([i, &serv, &file, flags, mode](){
                        return serv->nfi_open(file->m_path, flags, mode, file->m_data_vfh[i]);
                    });
                }
            }

            int aux_res;
            for (auto &fut : v_res)
            {
                if (!fut.valid()) continue;
                aux_res = fut.get();
                if (aux_res < 0)
                {
                    res = aux_res;
                    XPN_DEBUG_END_CUSTOM(path<<", "<<flags<<", "<<mode);
                    return res;
                }
            }

            #ifndef USE_ROCKSDB
            if ((O_DIRECTORY != (flags & O_DIRECTORY)))
            {
            #endif
                write_metadata(file->m_mdata, false);
            #ifndef USE_ROCKSDB
            }
            #endif
        }else{
            int master_file = file->m_mdata.master_file();
            std::future<int> fut;
            if ((O_DIRECTORY == (flags & O_DIRECTORY))){
                fut = m_worker->launch([&file, master_file, flags, mode](){
                    return file->m_part.m_data_serv[master_file]->nfi_opendir(file->m_path, file->m_data_vfh[master_file]);
                });
            }else{
                fut = m_worker->launch([&file, master_file, flags, mode](){
                    return file->m_part.m_data_serv[master_file]->nfi_open(file->m_path, flags, mode, file->m_data_vfh[master_file]);
                });
            }
            res = fut.get();
        }

        
        if ((O_DIRECTORY == (flags & O_DIRECTORY))){
            file->m_type = file_type::dir;
        }else{
            file->m_type = file_type::file;
        }
        file->m_flags = flags;
        file->m_mode = mode;
        res = m_file_table.insert(file);

        XPN_DEBUG_END_CUSTOM(path<<", "<<flags<<", "<<mode);
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

        std::vector<std::future<int>> v_res(file->m_data_vfh.size());
        for (size_t i = 0; i < file->m_data_vfh.size(); i++)
        {
            if (file->m_data_vfh[i].fd != -1)
            {
                v_res[i] = m_worker->launch([i, &file](){
                    return file->m_part.m_data_serv[i]->nfi_close(file->m_data_vfh[i]);
                });
            }
        }

        int aux_res;
        for (auto &fut : v_res)
        {   
            if (!fut.valid()) continue;
            aux_res = fut.get();
            if (aux_res < 0)
            {
                res = aux_res;
            }
        }
        
        m_file_table.remove(fd);

        XPN_DEBUG_END_CUSTOM(fd);
        return res;
    }

    int xpn_api::unlink(const char *path)
    {
        XPN_DEBUG_BEGIN_CUSTOM(path);
        int res = 0;

        std::string file_path;
        auto part_name = check_remove_part_from_path(path, file_path);
        if (part_name.empty()){
            errno = ENOENT;
            XPN_DEBUG_END_CUSTOM(path);
            return -1;
        }

        xpn_file file(file_path, m_partitions.at(part_name));

        res = read_metadata(file.m_mdata);
        if (res < 0){
            XPN_DEBUG_END_CUSTOM(path);
            return res;
        }

        std::vector<std::future<int>> v_res(file.m_part.m_data_serv.size());
        for (size_t i = 0; i < file.m_part.m_data_serv.size(); i++)
        {
            if (file.exist_in_serv(i)){
                v_res[i] = m_worker->launch([i, &v_res, &file](){
                    // Always wait and not async because it can fail in other ways
                    return file.m_part.m_data_serv[i]->nfi_remove(file.m_path, false);
                    // v_res[i] = file.m_part.m_data_serv[i]->nfi_remove(file.m_path, file.m_mdata.master_file()==static_cast<int>(i));
                });
            }
        }
        
        res = 0;
        int aux_res;
        for (auto &fut : v_res)
        {   
            if (!fut.valid()) continue;
            aux_res = fut.get();
            if (aux_res < 0)
            {
                res = aux_res;
            }
        }

        XPN_DEBUG_END_CUSTOM(path);
        return res;
    }

    int xpn_api::rename(const char *path, const char *newpath)
    {
        XPN_DEBUG_BEGIN_CUSTOM(path<<", "<<newpath);
        int res = 0;
        std::string file_path;
        auto part_name = check_remove_part_from_path(path, file_path);
        if (part_name.empty()){
            errno = ENOENT;
            XPN_DEBUG_END_CUSTOM(path<<", "<<newpath);
            return -1;
        }
        std::string new_file_path;
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

        xpn_file file(file_path, m_partitions.at(part_name));
        xpn_file new_file(new_file_path, m_partitions.at(new_part_name));

        res = read_metadata(file.m_mdata);
        if (res < 0){
            XPN_DEBUG_END_CUSTOM(path<<", "<<newpath);
            return res;
        }

        std::vector<std::future<int>> v_res(file.m_part.m_data_serv.size());
        for (size_t i = 0; i < file.m_part.m_data_serv.size(); i++)
        {
            if (file.exist_in_serv(i)){
                if (!new_file.exist_in_serv(i)){
                    XPN_DEBUG("Remove in server "<<i);
                    v_res[i] = m_worker->launch([i, &file, &new_file](){
                        return file.m_part.m_data_serv[i]->nfi_remove(file.m_path, false);
                    });
                }else{
                    XPN_DEBUG("Rename in server "<<i);
                    v_res[i] = m_worker->launch([i, &file, &new_file](){
                        return file.m_part.m_data_serv[i]->nfi_rename(file.m_path, new_file.m_path);
                    });
                }
            }
        }
        
        int aux_res;
        for (auto &fut : v_res)
        {   
            aux_res = fut.get();
            if (aux_res < 0)
            {
                res = aux_res;
            }
        }

        if (res >= 0){
            new_file.m_mdata.m_data = file.m_mdata.m_data;
            res = write_metadata(new_file.m_mdata, false);
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
