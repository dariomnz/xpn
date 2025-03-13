
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

#include <memory>

#include "coroutine/xpn_coroutine.hpp"
#include "xpn_server_ops.hpp"
#include "xpn_server_params.hpp"

namespace XPN {

class xpn_server_co_comm {
   public:
    static task<int64_t> read_operation(xpn_server_msg &msg, int comm_id, int &tag_client_id);
    static task<int64_t> read_data(void *data, int64_t size, int comm_id, int tag_client_id);
    static task<int64_t> write_data(const void *data, int64_t size, int comm_id, int tag_client_id);
};

class xpn_server_co_control_comm {
   public:
    xpn_server_co_control_comm();
    ~xpn_server_co_control_comm();

    task<int> accept(int socket);
    void disconnect(int comm_id);

   private:
    int m_server_comm;
    std::string m_port_name = std::string(MAX_PORT_NAME, '\0');
};
}  // namespace XPN