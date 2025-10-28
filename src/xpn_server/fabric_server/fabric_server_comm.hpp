
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
#include <memory>

#include "xpn_server/xpn_server_comm.hpp"
#include "lfi_async.h"

namespace XPN
{
  
  class fabric_server_comm : public xpn_server_comm
  {
  public:
    fabric_server_comm(int comm) : m_comm(comm) {}
    ~fabric_server_comm() override {}

    int64_t read_operation(xpn_server_msg &msg, int &rank_client_id, int &tag_client_id) override;
    int64_t read_data(void *data, int64_t size, int rank_client_id, int tag_client_id) override;
    int64_t write_data(const void *data, int64_t size, int rank_client_id, int tag_client_id) override;

    int64_t get_rank() override { return m_comm; }
  public:
    int m_comm;
  };
  
  class fabric_server_control_comm : public xpn_server_control_comm
  {
  public:
    fabric_server_control_comm();
    ~fabric_server_control_comm() override;
    
    std::shared_ptr<xpn_server_comm> accept(int socket, bool sendData = true) override;
    void disconnect(std::shared_ptr<xpn_server_comm> comm) override;
    
    std::shared_ptr<xpn_server_comm> create(int rank_client_id) override;
    int rearm(int rank_client_id) override;
    void disconnect(int rank_client_id) override;
    int64_t read_operation(xpn_server_msg &msg, int &rank_client_id, int &tag_client_id) override;
  private:
    int m_server_comm; 
    std::unique_ptr<lfi_request, void (*)(lfi_request *)> shm_request = {nullptr, nullptr};
    std::unique_ptr<lfi_request, void (*)(lfi_request *)> peer_request = {nullptr, nullptr};
    xpn_server_msg shm_msg = {};
    xpn_server_msg peer_msg = {};
  };

} // namespace XPN
