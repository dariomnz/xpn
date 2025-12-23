
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

#include "xpn/xpn_rw.hpp"

#include <algorithm>
#include <sstream>

#include "base_cpp/debug.hpp"
namespace XPN {

std::ostream &operator<<(std::ostream &os, xpn_rw_operation self) {
    switch (self.server_status) {
        case xpn_rw_operation::END:
            os << "END";
            break;
    }

    os << " srv " << self.server_status;
    os << " off " << self.srv_offset;

    os << " buf " << self.buffer;
    os << " size " << self.buffer_size;

    return os;
}

xpn_rw_calculator::xpn_rw_calculator(xpn_file &file, int64_t offset, const void *buffer, uint64_t size)
    : m_file(file),
      m_offset(offset),
      m_buffer(const_cast<uint8_t *>(static_cast<const uint8_t *>(buffer))),
      m_size(size),
      m_current_size(0),
      m_current_offset(offset),
      m_current_replication(0) {}

xpn_rw_operation xpn_rw_calculator::next_write() {
    xpn_rw_operation op = next_write_one();
    if (op.server_status == xpn_rw_operation::END) return op;

    // Need to pass to the next one if the server have error, to no make any request to that server
    bool server_with_error = m_file.m_part.m_data_serv[op.server_status]->m_error != 0;
    while (server_with_error) {
        op = next_write_one();
        if (op.server_status == xpn_rw_operation::END) return op;
        server_with_error = m_file.m_part.m_data_serv[op.server_status]->m_error != 0;
    }

    return op;
}

xpn_rw_operation xpn_rw_calculator::next_write_one() {
    uint64_t remaining_block_size = 0;
    xpn_rw_operation ret;

    XPN_DEBUG_BEGIN_CUSTOM("size " << m_current_size << ", "
                                   << "offset " << m_current_offset << ", "
                                   << "replication " << m_current_replication)
    // Check if is
    if (m_size <= m_current_size) {
        ret.server_status = xpn_rw_operation::END;
        return ret;
    }

    m_file.map_offset_mdata(m_current_offset, m_current_replication, ret.srv_offset, ret.server_status);

    // remaining_block_size is the remaining bytes from new_offset until the end of the block
    remaining_block_size = m_file.m_part.m_block_size - (m_current_offset % m_file.m_part.m_block_size);

    // If remaining_block_size > the remaining bytes to read/write, then adjust remaining_block_size
    if (remaining_block_size > (m_size - m_current_size)) {
        remaining_block_size = m_size - m_current_size;
    }

    ret.buffer = m_buffer + m_current_size;
    ret.buffer_size = remaining_block_size;

    m_current_replication++;

    // Addvance the offset only if the replication finish
    if (m_current_replication > m_file.m_part.m_replication_level) {
        m_current_replication = 0;
        m_current_size = remaining_block_size + m_current_size;
        m_current_offset = m_offset + m_current_size;
    }

    return ret;
}

xpn_rw_operation xpn_rw_calculator::next_read() {
    uint64_t remaining_block_size = 0;
    xpn_rw_operation ret;

    XPN_DEBUG_BEGIN_CUSTOM("size " << m_current_size << ", "
                                   << "offset " << m_current_offset)
    // Check if is
    if (m_size <= m_current_size) {
        ret.server_status = xpn_rw_operation::END;
        return ret;
    }

    read_get_block(m_file, m_current_offset, ret.srv_offset, ret.server_status);

    // remaining_block_size is the remaining bytes from new_offset until the end of the block
    remaining_block_size = m_file.m_part.m_block_size - (m_current_offset % m_file.m_part.m_block_size);

    // If remaining_block_size > the remaining bytes to read/write, then adjust remaining_block_size
    if (remaining_block_size > (m_size - m_current_size)) {
        remaining_block_size = m_size - m_current_size;
    }

    ret.buffer = m_buffer + m_current_size;
    ret.buffer_size = remaining_block_size;

    m_current_size = remaining_block_size + m_current_size;
    m_current_offset = m_offset + m_current_size;

    return ret;
}

// Calculate the read operation, with only one operation for each block for any replication level
uint64_t xpn_rw_calculator::max_ops_read() {
    // Avoid division by zero or size is 0
    if (m_file.m_part.m_block_size == 0 || m_size == 0) {
        return 0;
    }

    // Ceiling Division Formula: (a + b - 1) / b
    return (m_size + (m_file.m_part.m_block_size - 1)) / m_file.m_part.m_block_size;
}

// Calculate the write operations taking into account the replication level
uint64_t xpn_rw_calculator::max_ops_write() {
    // The maximun is the blocks in that size in case there are remaining and multiplied by the replication level
    return max_ops_read() * (m_file.m_part.m_replication_level + 1);
}

/**
 * Calculates the server and the offset (in server) for reads of the given offset (origin file) of a file with
 * replication.
 *
 * @param fd[in] A file descriptor.
 * @param offset[in] The original offset.
 * @param serv_client[in] To optimize: the server where the client is.
 * @param local_offset[out] The offset in the server.
 * @param serv[out] The server in which is located the given offset.
 *
 * @return Returns 0 on success or -1 on error.
 */
int xpn_rw_calculator::read_get_block(xpn_file &file, int64_t offset, int64_t &local_offset, int &serv) {
    int retries = 0;
    int replication = 0;
    int replication_level = file.m_part.m_replication_level;
    if (file.m_part.m_local_serv != -1) {
        do {
            file.map_offset_mdata(offset, replication, local_offset, serv);
            if (serv == file.m_part.m_local_serv && file.m_part.m_data_serv[serv]->m_error != -1) {
                return 0;
            }
            replication++;
        } while (replication <= replication_level);
    }

    replication = 0;
    if (replication_level != 0) replication = rand() % (replication_level + 1);

    do {
        file.map_offset_mdata(offset, replication, local_offset, serv);
        if (replication_level != 0) replication = (replication + 1) % (replication_level + 1);
        retries++;
    } while (file.m_part.m_data_serv[serv]->m_error == -1 && retries <= replication_level);

    return 0;
}
}  // namespace XPN