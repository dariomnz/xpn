
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

#include <ostream>
#include <vector>

#include "xpn/xpn_file.hpp"

namespace XPN {
struct xpn_rw_operation {
    enum status_t {
        SUCCESS = 0,
        END = -2,
    };
    int64_t srv_offset = -1;
    int32_t server_status = -1;

    uint32_t buffer_size = 0;
    void *buffer = nullptr;

    friend std::ostream &operator<<(std::ostream &os, xpn_rw_operation self);
};

class xpn_rw_calculator {
   public:
    xpn_rw_calculator(xpn_file &file, int64_t offset, const void *buffer, uint64_t size);

    static int read_get_block(xpn_file &file, int64_t offset, int64_t &local_offset, int &serv);

    xpn_rw_operation next_read();
    xpn_rw_operation next_write();
    xpn_rw_operation next_write_one();

    uint64_t max_ops_write();
    uint64_t max_ops_read();

    xpn_rw_operation recalcule_read();
    xpn_rw_operation recalcule_write();

   private:
    xpn_file &m_file;
    int64_t m_offset;
    uint8_t *m_buffer;
    uint64_t m_size;

   private:
    uint64_t m_current_size;
    int64_t m_current_offset;
    int32_t m_current_replication;
};
}  // namespace XPN