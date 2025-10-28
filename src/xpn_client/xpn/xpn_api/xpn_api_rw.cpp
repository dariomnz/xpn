
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
#include "xpn/xpn_rw.hpp"
#include <iomanip>

namespace XPN
{
    int64_t xpn_api::pread(int fd, void *buffer, uint64_t size, int64_t offset)
    {
        auto file = m_file_table.get(fd);
        if (!file){
            errno = EBADF;
            return -1;
        }

        return pread(file, buffer, size, offset);
    }

    int64_t xpn_api::pread(std::shared_ptr<xpn_file> file, void *buffer, uint64_t size, int64_t offset)
    {
        XPN_DEBUG_BEGIN_CUSTOM(file->m_path<<", "<<buffer<<", "<<size<<", "<<offset);
        int64_t res = 0;

        if (buffer == NULL){
            errno = EFAULT;
            res = -1;
            XPN_DEBUG_END_CUSTOM(file->m_path<<", "<<buffer<<", "<<size<<", "<<offset);
            return res;
        }

        if (size == 0){
            XPN_DEBUG_END_CUSTOM(file->m_path<<", "<<buffer<<", "<<size<<", "<<offset);
            return res;
        }

        if (file->m_flags == O_WRONLY){
            errno = EBADF;
            res = -1;
            XPN_DEBUG_END_CUSTOM(file->m_path<<", "<<buffer<<", "<<size<<", "<<offset);
            return res;
        }

        if (file->m_type == file_type::dir){
            errno = EISDIR;
            res = -1;
            XPN_DEBUG_END_CUSTOM(file->m_path<<", "<<buffer<<", "<<size<<", "<<offset);
            return res;
        }

        xpn_rw_buffer rw_buff(*file.get(), offset, const_cast<void*>(buffer), size);
        XPN_DEBUG("Before calculate_reads "<<rw_buff.dump());
        rw_buff.calculate_reads(xpn_env::get_instance().xpn_group_reads_writes);
        XPN_DEBUG("After calculate_reads "<<rw_buff.dump());

        std::vector<std::future<int>> v_res(rw_buff.num_ops());
        int index = 0;
        for (uint64_t i = 0; i < rw_buff.m_ops.size(); i++)
        {
            if (!rw_buff.m_ops[i].empty()){
                res = file->initialize_vfh(i);
                if (res < 0){
                    break;
                }   
            }
            for (auto &op : rw_buff.m_ops[i])
            {
                v_res[index++] = m_worker->launch([i, &file, &op](){
                    XPN_DEBUG("Serv "<<i<<" off: "<<op.offset_serv+xpn_metadata::HEADER_SIZE<<" size: "<<op.get_size());
                    auto ret = file->m_part.m_data_serv[i]->nfi_read(file->m_data_vfh[i], op.get_buffer(), op.offset_serv+xpn_metadata::HEADER_SIZE, op.get_size());
                    XPN_DEBUG("nfi_read "<<ret);
                    return ret;
                });
            }
        }
        
        uint64_t sum = 0;
        int aux_res;
        for (auto &fut : v_res)
        {
            if (!fut.valid()) continue;
            aux_res = fut.get();
            if (aux_res < 0){
                res = aux_res;
                XPN_DEBUG_END_CUSTOM(file->m_path<<", "<<buffer<<", "<<size);
                return res;
            }
            sum += aux_res;
        }

        res = sum;

        rw_buff.fix_ops_reads();

        XPN_DEBUG_END_CUSTOM(file->m_path<<", "<<buffer<<", "<<size<<", "<<offset);
        return res;
    }

    int64_t xpn_api::read(int fd, void *buffer, uint64_t size)
    {
        XPN_DEBUG_BEGIN_CUSTOM(fd<<", "<<buffer<<", "<<size);

        int64_t res = 0;

        auto file = m_file_table.get(fd);
        if (!file){
            errno = EBADF;
            res = -1;
            XPN_DEBUG_END_CUSTOM(fd<<", "<<buffer<<", "<<size);
            return res;
        }
        
        res = pread(file, buffer, size, file->m_offset);

        if(res > 0){
            XPN_DEBUG("Update offset " << file->m_offset << " -> " << file->m_offset + res);
            file->m_offset += res;
        }

        XPN_DEBUG_END_CUSTOM(fd<<", "<<buffer<<", "<<size);
        return res;
    }

    int64_t xpn_api::pwrite(int fd, const void *buffer, uint64_t size, int64_t offset)
    {
        auto file = m_file_table.get(fd);
        if (!file){
            errno = EBADF;
            return -1;
        }

        return pwrite(file, buffer, size, offset);
    }

    int64_t xpn_api::pwrite(std::shared_ptr<xpn_file> file, const void *buffer, uint64_t size, int64_t offset)
    {
        XPN_DEBUG_BEGIN_CUSTOM(file->m_path<<", "<<buffer<<", "<<size<<", "<<offset);
        int64_t res = 0;

        if (buffer == NULL){
            errno = EFAULT;
            res = -1;
            XPN_DEBUG_END_CUSTOM(file->m_path<<", "<<buffer<<", "<<size<<", "<<offset);
            return res;
        }

        if (size == 0){
            XPN_DEBUG_END_CUSTOM(file->m_path<<", "<<buffer<<", "<<size<<", "<<offset);
            return res;
        }

        if (file->m_flags == O_RDONLY){
            errno = EBADF;
            res = -1;
            XPN_DEBUG_END_CUSTOM(file->m_path<<", "<<buffer<<", "<<size<<", "<<offset);
            return res;
        }

        if (file->m_type == file_type::dir){
            errno = EISDIR;
            res = -1;
            XPN_DEBUG_END_CUSTOM(file->m_path<<", "<<buffer<<", "<<size<<", "<<offset);
            return res;
        }

        xpn_rw_buffer rw_buff(*file.get(), offset, const_cast<void*>(buffer), size);
        XPN_DEBUG("Before calculate_writes "<<rw_buff.dump());
        rw_buff.calculate_writes(xpn_env::get_instance().xpn_group_reads_writes);
        XPN_DEBUG("After calculate_writes "<<rw_buff.dump());

        std::vector<std::future<int>> v_res(rw_buff.num_ops());
        int index = 0;
        for (uint64_t i = 0; i < rw_buff.m_ops.size(); i++)
        {
            if (!rw_buff.m_ops[i].empty()){
                res = file->initialize_vfh(i);
                if (res < 0){
                    break;
                }
            }
            for (auto &op : rw_buff.m_ops[i])
            {
                v_res[index++] = m_worker->launch([i, &file, &op](){
                    XPN_DEBUG("Serv "<<i<<" off: "<<op.offset_serv+xpn_metadata::HEADER_SIZE<<" size: "<<op.get_size());
                    auto ret = file->m_part.m_data_serv[i]->nfi_write(file->m_data_vfh[i], op.get_buffer(), op.offset_serv+xpn_metadata::HEADER_SIZE, op.get_size());
                    XPN_DEBUG("nfi_write "<<ret);
                    return ret;
                });
            }
        }
        
        uint64_t sum = 0;
        int aux_res;
        for (auto &fut : v_res)
        {
            if (!fut.valid()) continue;
            aux_res = fut.get();
            if (aux_res < 0){
                XPN_DEBUG_END_CUSTOM(file->m_path<<", ops: "<<rw_buff.num_ops()<<", "<<rw_buff.size());
                return aux_res;
            }
            sum += aux_res;
        }

        if (sum != rw_buff.size()){
            res = sum / (file->m_part.m_replication_level+1);
        }else{
            res = static_cast<int64_t>(rw_buff.m_size);
            // Update file_size in metadata
            if ((offset+res) > static_cast<int64_t>(file->m_mdata.m_data.file_size)){
                file->m_mdata.m_data.file_size = offset+res;
                write_metadata(file->m_mdata, true);
            }
        }

        XPN_DEBUG_END_CUSTOM(file->m_path<<", "<<buffer<<", "<<size<<", "<<offset);
        return res;
    }

    int64_t xpn_api::write(int fd, const void *buffer, uint64_t size)
    {
        XPN_DEBUG_BEGIN_CUSTOM(fd<<", "<<buffer<<", "<<size);

        int64_t res = 0;

        auto file = m_file_table.get(fd);
        if (!file){
            errno = EBADF;
            res = -1;
            XPN_DEBUG_END_CUSTOM(fd<<", "<<buffer<<", "<<size);
            return res;
        }
        
        res = pwrite(file, buffer, size, file->m_offset);

        if(res > 0){
            file->m_offset += res;
        }

        XPN_DEBUG_END_CUSTOM(fd<<", "<<buffer<<", "<<size);
        return res;
    }

    int64_t xpn_api::lseek(int fd, int64_t offset, int flag)
    {
        XPN_DEBUG_BEGIN_CUSTOM(fd<<", "<<offset<<", "<<flag);
        int64_t res = 0;
        struct ::stat st;

        auto file = m_file_table.get(fd);
        if (!file){
            errno = EBADF;
            res = -1;
            XPN_DEBUG_END_CUSTOM(fd<<", "<<offset<<", "<<flag);
            return res;
        }
        
        switch (flag)
        {
            case SEEK_SET:
                if (offset < 0)
                {
                    errno = EINVAL;
                    res = -1;
                    XPN_DEBUG_END_CUSTOM(fd<<", "<<offset<<", "<<flag);
                    return res;
                }
                else {
                    file->m_offset = offset;
                }
                break;

            case SEEK_CUR:
                if (file->m_offset+offset<0)
                {
                    errno = EINVAL;
                    res = -1;
                    XPN_DEBUG_END_CUSTOM(fd<<", "<<offset<<", "<<flag);
                    return res;
                }
                else {
                    file->m_offset += offset;
                }
                break;

            case SEEK_END:
                if (fstat(fd, &st)<0)
                {
                    errno = EBADF;
                    res = -1;
                    XPN_DEBUG_END_CUSTOM(fd<<", "<<offset<<", "<<flag);
                    return res;
                }
                if (st.st_size + offset<0)
                {
                    errno = EINVAL;
                    res = -1;
                    XPN_DEBUG_END_CUSTOM(fd<<", "<<offset<<", "<<flag);
                    return res;
                }
                else {
                    file->m_offset = st.st_size + offset;
                }
            break;

            default:
                errno = EINVAL;
                res = -1;
                XPN_DEBUG_END_CUSTOM(fd<<", "<<offset<<", "<<flag);
                return res;
        }

        res = file->m_offset;
        XPN_DEBUG_END_CUSTOM(fd<<", "<<offset<<", "<<flag);
        return res;
    }
} // namespace XPN
