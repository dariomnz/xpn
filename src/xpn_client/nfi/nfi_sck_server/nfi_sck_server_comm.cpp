
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

#include "nfi_sck_server_comm.hpp"
#include "xpn_server/xpn_server_params.hpp"
#include "base_cpp/debug.hpp"
#include "base_cpp/socket.hpp"
#include "base_cpp/ns.hpp"
#include <csignal>
#include <xpn_server/xpn_server_ops.hpp>

#ifdef ENABLE_MQTT_SERVER
#include "../nfi_mqtt_server/nfi_mqtt_server_comm.hpp"
#endif

namespace XPN {

std::unique_ptr<nfi_xpn_server_comm> nfi_sck_server_control_comm::control_connect ( const std::string &srv_name, int srv_port )
{
  int ret;
  int connection_socket;
  char port_name[MAX_PORT_NAME];

  debug_info("[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_connect] >> Begin");
  
  debug_info("[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_connect] srv_name '"<<srv_name<<"' srv_port '"<<srv_port<<"'");
  // Lookup port name
  ret = socket::client_connect(srv_name, srv_port,
                          xpn_env::get_instance().xpn_connect_timeout_ms,
                          connection_socket,
                          xpn_env::get_instance().xpn_connect_retry_time_ms);
  if (ret < 0)
  {
    debug_error("[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_connect] ERROR: socket connect\n");
    return nullptr;
  }
  int buffer = socket::xpn_server::ACCEPT_CODE;
  ret = socket::send(connection_socket, &buffer, sizeof(buffer));
  if (ret < 0)
  {
    debug_error("[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_connect] ERROR: socket send\n");
    socket::close(connection_socket);
    return nullptr;
  }
  ret = socket::recv(connection_socket, port_name, MAX_PORT_NAME);
  if (ret < 0)
  {
    debug_error("[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_connect] ERROR: socket read\n");
    socket::close(connection_socket);
    return nullptr;
  }
  socket::close(connection_socket);

  if (ret < 0) {
    printf("[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_connect] ERROR: Lookup %s Port %s\n", srv_name.c_str(), port_name);
    return nullptr;
  }

  debug_info("[NFI_SCK_SERVER_COMM] ----SERVER = "<<srv_name<<" PORT = "<<port_name);

  // Connect...
  return connect(srv_name, port_name);
}

std::unique_ptr<nfi_xpn_server_comm> nfi_sck_server_control_comm::connect(const std::string &srv_name, const std::string& port_name) {
  int ret, sd;
  // Connect...
  debug_info("[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_connect] Connect port "<<port_name);

  ret = socket::client_connect(srv_name, atoi(port_name.c_str()), sd);
  if (ret < 0) {
    fprintf(stderr, "[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_connect] ERROR: client_connect(%s,%s)\n", srv_name.c_str(), port_name.c_str());
    return nullptr;
  }

  void* res_mqtt = nullptr;
  if (m_is_mqtt) {
    #ifdef ENABLE_MQTT_SERVER
    mosquitto * mqtt = nullptr;
    nfi_mqtt_server::init(&mqtt, srv_name);
    res_mqtt = mqtt;
    #endif
  }

  debug_info("[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_connect] << End\n");

  return std::make_unique<nfi_sck_server_comm>(sd, res_mqtt);
}

void nfi_sck_server_control_comm::disconnect(std::unique_ptr<nfi_xpn_server_comm> &comm, bool needSendCode) 
{
  int ret;
  nfi_sck_server_comm *in_comm = static_cast<nfi_sck_server_comm*>(comm.get());

  debug_info("[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_disconnect] >> Begin");

  if (needSendCode){
    debug_info("[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_disconnect] Send disconnect message");  
    xpn_server_msg msg = {};
    msg.op = static_cast<int>(xpn_server_ops::DISCONNECT);
    msg.msg_size = 0;
    ret = in_comm->write_operation(msg);
    if (ret < 0) {
      printf("[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_disconnect] ERROR: nfi_sck_server_comm_write_operation fails");
    }
  }

  // Disconnect
  debug_info("[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_disconnect] Disconnect");

  ret = socket::close(in_comm->m_socket);
  if (ret < 0) {
    printf("[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_disconnect] ERROR: MPI_Comm_disconnect fails");
  }
  
  if (m_is_mqtt && in_comm->m_mqtt) {
    #ifdef ENABLE_MQTT_SERVER
    nfi_mqtt_server::destroy(static_cast<mosquitto*>(in_comm->m_mqtt));
    #endif
  }

  comm.reset();

  debug_info("[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_disconnect] << End");
}

int64_t nfi_sck_server_comm::write_operation(xpn_server_msg& msg) {
    int ret;

    debug_info("[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_write_operation] >> Begin");

    // Message generation
    msg.tag = (int)(pthread_self() % 32450) + 1;

    // Send message
    debug_info("[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_write_operation] Write operation send tag "<< msg.tag);

    ret = socket::send(m_socket, &msg, msg.get_size());
    if (ret < 0) {
        debug_error("[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_write_operation] ERROR: socket::send < 0 : "<< ret);
        return -1;
    }

    debug_info("[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_write_operation] << End");

    // Return OK
    return 0;
}

int64_t nfi_sck_server_comm::write_data(const void *data, int64_t size) {
    int ret;

    debug_info("[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_write_data] >> Begin");

    // Check params
    if (size == 0) {
        return 0;
    }
    if (size < 0) {
        printf("[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_write_data] ERROR: size < 0");
        return -1;
    }

    // Send message
    debug_info("[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_write_data] Write data size "<<size);

    ret = socket::send(m_socket, data, size);
    if (ret < 0) {
        printf("[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_write_data] ERROR: MPI_Send fails");
        size = 0;
    }

    debug_info("[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_write_data] << End");

    // Return bytes written
    return size;
}

int64_t nfi_sck_server_comm::read_data(void *data, int64_t size) {
    int ret;

    debug_info("[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_read_data] >> Begin");

    // Check params
    if (size == 0) {
        return 0;
    }
    if (size < 0) {
        printf("[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_read_data] ERROR: size < 0");
        return -1;
    }

    // Get message
    debug_info("[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_read_data] Read data size "<<size);

    ret = socket::recv(m_socket, data, size);
    if (ret < 0) {
        printf("[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_read_data] ERROR: MPI_Recv fails");
        size = 0;
    }

    debug_info("[NFI_SCK_SERVER_COMM] [nfi_sck_server_comm_read_data] << End");

    // Return bytes read
    return size;
}

} //namespace XPN
