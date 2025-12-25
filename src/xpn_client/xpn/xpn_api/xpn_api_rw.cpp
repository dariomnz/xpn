
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

        return pread(*file, buffer, size, offset);
    }

    int64_t xpn_api::pread(xpn_file& file, void *buffer, uint64_t size, int64_t offset)
    {
        XPN_DEBUG_BEGIN_CUSTOM(file.m_path<<", "<<buffer<<", "<<size<<", "<<offset);
        int64_t res = 0;

        if (buffer == NULL){
            errno = EFAULT;
            res = -1;
            XPN_DEBUG_END_CUSTOM(file.m_path<<", "<<buffer<<", "<<size<<", "<<offset);
            return res;
        }

        if (size == 0){
            XPN_DEBUG_END_CUSTOM(file.m_path<<", "<<buffer<<", "<<size<<", "<<offset);
            return res;
        }

        if (file.m_flags == O_WRONLY){
            errno = EBADF;
            res = -1;
            XPN_DEBUG_END_CUSTOM(file.m_path<<", "<<buffer<<", "<<size<<", "<<offset);
            return res;
        }

        if (file.m_type == file_type::dir){
            errno = EISDIR;
            res = -1;
            XPN_DEBUG_END_CUSTOM(file.m_path<<", "<<buffer<<", "<<size<<", "<<offset);
            return res;
        }

        xpn_rw_calculator rw_calculator(file, offset, buffer, size);

        xpn_rw_operation rw_op;
        rw_op.server_status = xpn_rw_operation::SUCCESS;
        
        uint64_t sum = 0;
        FixedTaskQueue<WorkerResult> tasks;
        while(rw_op.server_status != xpn_rw_operation::END) {
            rw_op = rw_calculator.next_read();
            XPN_DEBUG(rw_op);
            if (rw_op.server_status == xpn_rw_operation::END){
                break;
            }
            
            res = file.initialize_vfh(rw_op.server_status);
            if (res < 0){
                break;
            }
            if (tasks.full()) {
                auto aux_res = tasks.consume_one();
                if (aux_res.result < 0){
                    res = aux_res.result;
                    errno = aux_res.errorno;
                    XPN_DEBUG_END_CUSTOM(file.m_path<<", "<<buffer<<", "<<size);
                    return res;
                }
                sum += aux_res.result;
            }
            auto &task = tasks.get_next_slot();
            m_worker->launch([&file, rw_op]() {
                XPN_DEBUG("Serv " << rw_op.server_status << " off: " << rw_op.srv_offset + xpn_metadata::HEADER_SIZE
                                  << " size: " << rw_op.buffer_size);
                auto ret = file.m_part.m_data_serv[rw_op.server_status]->nfi_read(
                    file.m_path, file.m_data_vfh[rw_op.server_status], static_cast<char *>(rw_op.buffer),
                    rw_op.srv_offset + xpn_metadata::HEADER_SIZE, rw_op.buffer_size);
                XPN_DEBUG("nfi_read " << ret);
                return WorkerResult(ret);
            }, task);
        }

        while (!tasks.empty()) {
            auto aux_res = tasks.consume_one();
            if (aux_res.result < 0){
                res = aux_res.result;
                errno = aux_res.errorno;
                XPN_DEBUG_END_CUSTOM(file.m_path<<", "<<buffer<<", "<<size);
                return res;
            }
            sum += aux_res.result;
        }

        res = sum;

        XPN_DEBUG_END_CUSTOM(file.m_path<<", "<<buffer<<", "<<size<<", "<<offset);
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
        
        res = pread(*file, buffer, size, file->m_offset);

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

        return pwrite(*file, buffer, size, offset);
    }

    int64_t xpn_api::pwrite(xpn_file& file, const void *buffer, uint64_t size, int64_t offset)
    {
        XPN_DEBUG_BEGIN_CUSTOM(file.m_path<<", "<<buffer<<", "<<size<<", "<<offset);
        int64_t res = 0;

        if (buffer == NULL){
            errno = EFAULT;
            res = -1;
            XPN_DEBUG_END_CUSTOM(file.m_path<<", "<<buffer<<", "<<size<<", "<<offset);
            return res;
        }

        if (size == 0){
            XPN_DEBUG_END_CUSTOM(file.m_path<<", "<<buffer<<", "<<size<<", "<<offset);
            return res;
        }

        if (file.m_flags == O_RDONLY){
            errno = EBADF;
            res = -1;
            XPN_DEBUG_END_CUSTOM(file.m_path<<", "<<buffer<<", "<<size<<", "<<offset);
            return res;
        }

        if (file.m_type == file_type::dir){
            errno = EISDIR;
            res = -1;
            XPN_DEBUG_END_CUSTOM(file.m_path<<", "<<buffer<<", "<<size<<", "<<offset);
            return res;
        }

        xpn_rw_calculator rw_calculator(file, offset, buffer, size);

        uint64_t intended_size=0;
        xpn_rw_operation rw_op;
        rw_op.server_status = xpn_rw_operation::SUCCESS;
        
        uint64_t sum = 0;
        FixedTaskQueue<WorkerResult> tasks;
        while(rw_op.server_status != xpn_rw_operation::END) {
            rw_op = rw_calculator.next_write();
            XPN_DEBUG(rw_op);
            if (rw_op.server_status == xpn_rw_operation::END){
                break;
            }
            
            res = file.initialize_vfh(rw_op.server_status);
            if (res < 0){
                break;
            }

            intended_size += rw_op.buffer_size;
            if (tasks.full()) {
                auto aux_res = tasks.consume_one();
                if (aux_res.result < 0){
                    res = aux_res.result;
                    errno = aux_res.errorno;
                    XPN_DEBUG_END_CUSTOM(file.m_path<<", "<<buffer<<", "<<size);
                    return res;
                }
                sum += aux_res.result;
            }
            auto &task = tasks.get_next_slot();
            m_worker->launch([&file, rw_op]() {
                XPN_DEBUG("Serv " << rw_op.server_status << " off: " << rw_op.srv_offset + xpn_metadata::HEADER_SIZE
                                  << " size: " << rw_op.buffer_size);
                auto ret = file.m_part.m_data_serv[rw_op.server_status]->nfi_write(
                    file.m_path, file.m_data_vfh[rw_op.server_status], static_cast<char *>(rw_op.buffer),
                    rw_op.srv_offset + xpn_metadata::HEADER_SIZE, rw_op.buffer_size);
                XPN_DEBUG("nfi_write " << ret);
                return WorkerResult(ret);
            }, task);
        }

        while (!tasks.empty()) {
            auto aux_res = tasks.consume_one();
            if (aux_res.result < 0){
                res = aux_res.result;
                errno = aux_res.errorno;
                XPN_DEBUG_END_CUSTOM(file.m_path<<", "<<buffer<<", "<<size);
                return res;
            }
            sum += aux_res.result;
        }

        if (sum != intended_size){
            res = sum / (file.m_part.m_replication_level+1);
        }else{
            res = static_cast<int64_t>(size);
            // Update file_size in metadata
            if ((offset+res) > static_cast<int64_t>(file.m_mdata.m_data.file_size)){
                file.m_mdata.m_data.file_size = offset+res;
                write_metadata(file.m_mdata, true);
            }
        }

        XPN_DEBUG_END_CUSTOM(file.m_path<<", "<<buffer<<", "<<size<<", "<<offset);
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
        
        res = pwrite(*file, buffer, size, file->m_offset);

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
