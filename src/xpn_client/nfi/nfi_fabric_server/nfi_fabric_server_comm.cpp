
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

#include "nfi_fabric_server_comm.hpp"
#include "xpn_server/xpn_server_params.hpp"
#include "base_cpp/debug.hpp"
#include "base_cpp/socket.hpp"
#include "base_cpp/ns.hpp"
#include <csignal>
#include <xpn_server/xpn_server_ops.hpp>
#include "lfi.h"

namespace XPN {

nfi_xpn_server_comm* nfi_fabric_server_control_comm::connect ( const std::string &srv_name )
{
  int ret;
  int connection_socket;
  char port_name[MAX_PORT_NAME];

  debug_info("[NFI_FABRIC_SERVER_COMM] [nfi_fabric_server_comm_connect] >> Begin");

  // Lookup port name
  ret = socket::client_connect(srv_name, xpn_env::get_instance().xpn_sck_port, xpn_env::get_instance().xpn_connect_timeout_ms, connection_socket);
  if (ret < 0)
  {
    debug_error("[NFI_FABRIC_SERVER_COMM] [nfi_fabric_server_comm_connect] ERROR: socket connect\n");
    return nullptr;
  }
  int buffer = socket::xpn_server::ACCEPT_CODE;
  ret = socket::send(connection_socket, &buffer, sizeof(buffer));
  if (ret < 0)
  {
    debug_error("[NFI_FABRIC_SERVER_COMM] [nfi_fabric_server_comm_connect] ERROR: socket send\n");
    socket::close(connection_socket);
    return nullptr;
  }
  ret = socket::recv(connection_socket, port_name, MAX_PORT_NAME);
  if (ret < 0)
  {
    debug_error("[NFI_FABRIC_SERVER_COMM] [nfi_fabric_server_comm_connect] ERROR: socket read\n");
    socket::close(connection_socket);
    return nullptr;
  }
  socket::close(connection_socket);

  if (ret < 0) {
    printf("[NFI_FABRIC_SERVER_COMM] [nfi_fabric_server_comm_connect] ERROR: Lookup %s Port %s\n", srv_name.c_str(), port_name);
    return nullptr;
  }

  debug_info("[NFI_FABRIC_SERVER_COMM] ----SERVER = "<<srv_name<<" PORT = "<<port_name);

  int new_comm = lfi_client_create(srv_name.c_str(), atoi(port_name));

  if (new_comm < 0)
  {
    printf("[NFI_FABRIC_SERVER_COMM] [nfi_fabric_server_comm_connect] ERROR: connect fails\n");
    return nullptr;
  }

  debug_info("[NFI_FABRIC_SERVER_COMM] [nfi_fabric_server_comm_connect] << End\n");

  return new (std::nothrow) nfi_fabric_server_comm(new_comm);
}

void nfi_fabric_server_control_comm::disconnect(nfi_xpn_server_comm *comm) 
{
  int ret;
  nfi_fabric_server_comm *in_comm = static_cast<nfi_fabric_server_comm*>(comm);

  debug_info("[NFI_FABRIC_SERVER_COMM] [nfi_fabric_server_comm_disconnect] >> Begin");

  debug_info("[NFI_FABRIC_SERVER_COMM] [nfi_fabric_server_comm_disconnect] Send disconnect message");
  xpn_server_msg msg = {};
  msg.op = static_cast<int>(xpn_server_ops::DISCONNECT);
  msg.msg_size = 0;
  ret = in_comm->write_operation(msg);
  if (ret < 0) {
    printf("[NFI_FABRIC_SERVER_COMM] [nfi_fabric_server_comm_disconnect] ERROR: nfi_fabric_server_comm_write_operation fails");
  }

  // Disconnect
  debug_info("[NFI_FABRIC_SERVER_COMM] [nfi_fabric_server_comm_disconnect] Disconnect");
  
  ret = lfi_client_close(in_comm->m_comm);
  if (ret < 0) {
    printf("[NFI_FABRIC_SERVER_COMM] [nfi_fabric_server_comm_disconnect] ERROR: MPI_Comm_disconnect fails");
  }

  delete comm;

  debug_info("[NFI_FABRIC_SERVER_COMM] [nfi_fabric_server_comm_disconnect] << End");
}

int64_t nfi_fabric_server_comm::write_operation(xpn_server_msg& msg) {
    int ret;

    debug_info("[NFI_FABRIC_SERVER_COMM] [nfi_fabric_server_comm_write_operation] >> Begin");

    // Message generation
    msg.tag = (int)(pthread_self() % 32450) + 1;

    // Send message
    debug_info("[NFI_FABRIC_SERVER_COMM] [nfi_fabric_server_comm_write_operation] Write operation send tag "<< msg.tag);

    ret = lfi_tsend(m_comm, &msg, msg.get_size(), 0);
    if (ret < 0) {
        debug_error("[NFI_FABRIC_SERVER_COMM] [nfi_fabric_server_comm_write_operation] ERROR: socket::send < 0 : "<< ret);
        return -1;
    }

    debug_info("[NFI_FABRIC_SERVER_COMM] [nfi_fabric_server_comm_write_operation] << End");

    // Return OK
    return 0;
}

int64_t nfi_fabric_server_comm::write_data(const void *data, int64_t size) {
    int ret;

    debug_info("[NFI_FABRIC_SERVER_COMM] [nfi_fabric_server_comm_write_data] >> Begin");

    // Check params
    if (size == 0) {
        return 0;
    }
    if (size < 0) {
        printf("[NFI_FABRIC_SERVER_COMM] [nfi_fabric_server_comm_write_data] ERROR: size < 0");
        return -1;
    }

    int tag = (int)(pthread_self() % 32450) + 1;

    // Send message
    debug_info("[NFI_FABRIC_SERVER_COMM] [nfi_fabric_server_comm_write_data] Write data");

    ret = lfi_tsend(m_comm, data, size, tag);
    if (ret < 0) {
        printf("[NFI_MPI_SERVER_COMM] [nfi_mpi_server_comm_write_data] ERROR: MPI_Send fails");
        size = 0;
    }

    debug_info("[NFI_FABRIC_SERVER_COMM] [nfi_fabric_server_comm_write_data] << End");

    // Return bytes written
    return size;
}

int64_t nfi_fabric_server_comm::read_data(void *data, ssize_t size) {
    int ret;

    debug_info("[NFI_FABRIC_SERVER_COMM] [nfi_fabric_server_comm_read_data] >> Begin");

    // Check params
    if (size == 0) {
        return 0;
    }
    if (size < 0) {
        printf("[NFI_FABRIC_SERVER_COMM] [nfi_fabric_server_comm_read_data] ERROR: size < 0");
        return -1;
    }

    int tag = (int)(pthread_self() % 32450) + 1;

    // Get message
    debug_info("[NFI_FABRIC_SERVER_COMM] [nfi_fabric_server_comm_read_data] Read data");

    ret = lfi_trecv(m_comm, data, size, tag);
    if (ret < 0) {
        printf("[NFI_FABRIC_SERVER_COMM] [nfi_fabric_server_comm_read_data] ERROR: MPI_Recv fails");
        size = 0;
    }

    debug_info("[NFI_FABRIC_SERVER_COMM] [nfi_fabric_server_comm_read_data] << End");

    // Return bytes read
    return size;
}

} //namespace XPN
