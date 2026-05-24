
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
#include <bitset>

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

        if (buffer == NULL || size == 0 || file.m_flags == O_WRONLY || file.m_type == file_type::dir) {
            if (size == 0) { res = 0; }
            if (buffer == NULL) { errno = EFAULT; res = -1; }
            if (file.m_flags == O_WRONLY) { errno = EBADF; res = -1; }
            if (file.m_type == file_type::dir) { errno = EISDIR; res = -1; }
            XPN_DEBUG_END_CUSTOM(file.m_path<<", "<<buffer<<", "<<size<<", "<<offset);
            return res;
        }

        xpn_rw_calculator rw_calculator(file, offset, buffer, size);

        xpn_rw_operation rw_op;
        rw_op.server_status = xpn_rw_operation::SUCCESS;
        uint64_t sum = 0;

        auto result_handler = [&](const WorkerResult &r) {
            if (r.result < 0){
                res = r.result;
                errno = r.errorno;
                return false; // Stop
            }
            sum += r.result;
            return true; // Continue
        };

        FixedTaskQueue tasks(*m_worker, result_handler);
        while (rw_op.server_status != xpn_rw_operation::END) {
            rw_op = rw_calculator.next_read();
            XPN_DEBUG(rw_op);
            if (rw_op.server_status == xpn_rw_operation::END) {
                break;
            }

            bool ok = tasks.launch([&file, &rw_calculator, rw_op]() {
                xpn_rw_operation current_op = rw_op;
                
                while (current_op.server_status != xpn_rw_operation::END) {
                    int64_t ret = -1;
                    if (file.initialize_vfh(current_op.server_status) >= 0) {
                        ret = file.m_part.m_data_serv[current_op.server_status]->nfi_read(
                            file, file.m_data_vfh[current_op.server_status], static_cast<char *>(current_op.buffer),
                            current_op.srv_offset + xpn_metadata::HEADER_SIZE, current_op.buffer_size);
                    }
                    if (ret >= 0) {
                        return WorkerResult(ret);
                    } else if (file.m_part.m_data_serv[current_op.server_status]->m_error >= 0) {
                        return WorkerResult(ret);
                    }

                    XPN_DEBUG("Fail in serv "<<current_op.server_status<<". Searching for replica...");
                    current_op = rw_calculator.next_replica(current_op);
                }

                return WorkerResult(-1);
            });

            if (!ok) {
                XPN_DEBUG_END_CUSTOM(file.m_path<<", "<<buffer<<", "<<size);
                return res;
            }
        }

        if (!tasks.wait_remaining()) {
            XPN_DEBUG_END_CUSTOM(file.m_path<<", "<<buffer<<", "<<size);
            return res;
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

        if (buffer == NULL || size == 0 || file.m_flags == O_RDONLY || file.m_type == file_type::dir) {
            if (size == 0) { res = 0; }
            if (buffer == NULL) { errno = EFAULT; res = -1; }
            if (file.m_flags == O_RDONLY) { errno = EBADF; res = -1; }
            if (file.m_type == file_type::dir) { errno = EISDIR; res = -1; }
            XPN_DEBUG_END_CUSTOM(file.m_path<<", "<<buffer<<", "<<size<<", "<<offset);
            return res;
        }

        constexpr uint32_t WINDOW_SIZE = 64;
        std::atomic<uint64_t> logical_block_bitmap{0};
        bool global_error = false;
        int last_errno = 0;

        FixedTaskQueue tasks(*m_worker, [&](const WorkerResult &r) {
            if (r.result < 0) last_errno = r.errorno;
            return true;
        });
        xpn_rw_calculator rw_calculator(file, offset, buffer, size);
        xpn_rw_operation rw_op;
        rw_op.server_status = xpn_rw_operation::SUCCESS;

        while (rw_op.server_status != xpn_rw_operation::END && !global_error) {
            logical_block_bitmap.store(0, std::memory_order_relaxed);
            uint32_t current_window_blocks = 0;

            // while one iter per burst of 64 blocks
            while (rw_op.server_status != xpn_rw_operation::END && current_window_blocks < WINDOW_SIZE) {
                const void *current_block_ptr = rw_op.buffer;
                uint32_t block_window_index = current_window_blocks;

                // while for replicas for THIS specific logical block atomically
                while (rw_op.server_status != xpn_rw_operation::END && rw_op.buffer == current_block_ptr) {
                    rw_op = rw_calculator.next_write();

                    bool ok = tasks.launch([&file, rw_op, block_window_index, &logical_block_bitmap]() {
                        XPN_DEBUG("Serv " << rw_op.server_status
                                          << " off: " << rw_op.srv_offset + xpn_metadata::HEADER_SIZE);
                        if (file.initialize_vfh(rw_op.server_status) < 0) {
                            return WorkerResult(0);
                        }
                        auto ret = file.m_part.m_data_serv[rw_op.server_status]->nfi_write(
                            file, file.m_data_vfh[rw_op.server_status], static_cast<const char *>(rw_op.buffer),
                            rw_op.srv_offset + xpn_metadata::HEADER_SIZE, rw_op.buffer_size);

                        if (ret >= 0) {
                            uint64_t mask = 1ULL << block_window_index;
                            logical_block_bitmap.fetch_or(mask, std::memory_order_relaxed);
                        }
                        return WorkerResult(ret);
                    });

                    if (!ok) {
                        XPN_DEBUG_END_CUSTOM(file.m_path << ", " << buffer << ", " << size);
                        return -1;
                    }
                }
                if (rw_op.server_status != xpn_rw_operation::END) current_window_blocks++;
            }

            if (!tasks.wait_remaining()) {
                XPN_DEBUG_END_CUSTOM(file.m_path << ", " << buffer << ", " << size);
                return -1;
            }

            if (current_window_blocks > 0) {
                uint64_t expected_mask = (current_window_blocks == 64) ? ~0ULL : (1ULL << current_window_blocks) - 1;
                uint64_t actual_bitmap = logical_block_bitmap.load(std::memory_order_relaxed);
                XPN_DEBUG("Actual bitmap " << std::bitset<64>(actual_bitmap) << " "
                                           << std::bitset<64>(actual_bitmap).count());
                if ((actual_bitmap & expected_mask) != expected_mask) {
                    XPN_DEBUG("CRITICAL: A logical block in this window lost ALL its replicas "<<current_window_blocks);
                    global_error = true;
                }
            }
        }

        if (global_error) {
            errno = last_errno ? last_errno : EIO;
            return -1;
        }

        res = static_cast<int64_t>(size);
        if ((offset + res) > static_cast<int64_t>(file.m_mdata.m_data.file_size)) {
            file.m_mdata.m_data.file_size = offset + res;
            write_metadata(file.m_mdata, true);
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
