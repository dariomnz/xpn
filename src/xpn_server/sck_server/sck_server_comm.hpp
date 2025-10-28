
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

namespace XPN
{
  
  class sck_server_comm : public xpn_server_comm
  {
  public:
    sck_server_comm(int socket) : m_socket(socket) {}
    ~sck_server_comm() override {}

    int64_t read_operation(xpn_server_msg &msg, int &rank_client_id, int &tag_client_id) override;
    int64_t read_data(void *data, int64_t size, int rank_client_id, int tag_client_id) override;
    int64_t write_data(const void *data, int64_t size, int rank_client_id, int tag_client_id) override;

    int64_t get_rank() override { return m_socket; }
  public:
    int m_socket;
  };
  
  class sck_server_control_comm : public xpn_server_control_comm
  {
  public:
    sck_server_control_comm(xpn_server_params &params, int port);
    ~sck_server_control_comm() override;
    
    std::shared_ptr<xpn_server_comm> accept(int socket, bool sendData = true) override;
    void disconnect(std::shared_ptr<xpn_server_comm> comm) override;
    
    std::shared_ptr<xpn_server_comm> create(int rank_client_id) override;
    int rearm(int rank_client_id) override;
    void disconnect(int rank_client_id) override;
    int64_t read_operation(xpn_server_msg &msg, int &rank_client_id, int &tag_client_id) override;
  private:
    int m_socket;
    int m_epoll;

  public:
    void* m_mqtt = nullptr;
  };

} // namespace XPN
