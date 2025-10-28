
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

#include "fabric_server_comm.hpp"
#include "base_cpp/debug.hpp"
#include "base_cpp/timer.hpp"
#include "base_cpp/ns.hpp"
#include "base_cpp/socket.hpp"
#include <csignal>
#include "lfi.h"
#include "lfi_async.h"
#include <cassert>

namespace XPN
{
fabric_server_control_comm::fabric_server_control_comm ()
{
  XPN_PROFILE_FUNCTION();
  debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_control_comm] >> Begin");
  m_type = server_type::FABRIC;
  
  int port = 0;
  m_server_comm = lfi_server_create(NULL, &port);

  m_port_name = std::to_string(port);

  if (m_server_comm < 0)
  {
    print("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_comm_init] ERROR: bind fails");
    std::raise(SIGTERM);
  }

  debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_control_comm] >> End");
}

fabric_server_control_comm::~fabric_server_control_comm()
{
  XPN_PROFILE_FUNCTION();
  debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [~fabric_server_control_comm] >> Begin");

  lfi_server_close(m_server_comm);

  debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [~fabric_server_control_comm] >> End");
}

std::shared_ptr<xpn_server_comm> fabric_server_control_comm::accept ( int socket, bool sendData )
{
  XPN_PROFILE_FUNCTION();
  debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_comm_accept] >> Begin");
  
  int ret = 0;
  if (sendData) {
    debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_comm_accept] send port");
    ret = socket::send(socket, m_port_name.data(), MAX_PORT_NAME);
    if (ret < 0){
      print("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_CONTROL_COMM] [fabric_server_control_comm_accept] ERROR: socket send port fails");
      return nullptr;
    }
  }

  int new_comm = lfi_server_accept(m_server_comm);
  if (new_comm < 0){
    print("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_CONTROL_COMM] [fabric_server_control_comm_accept] ERROR: accept fails");
    return nullptr;
  }

  debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_CONTROL_COMM] [fabric_server_control_comm_accept] << End");

  return std::make_shared<fabric_server_comm>(new_comm);
}

std::shared_ptr<xpn_server_comm> fabric_server_control_comm::create ( int rank_client_id ) {
  return std::make_shared<fabric_server_comm>(rank_client_id);
}

int fabric_server_control_comm::rearm([[maybe_unused]] int rank_client_id) {
  return 0;
}

void fabric_server_control_comm::disconnect ( std::shared_ptr<xpn_server_comm> comm )
{
  XPN_PROFILE_FUNCTION();
  debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_comm_disconnect] >> Begin");
  
  fabric_server_comm *in_comm = static_cast<fabric_server_comm*>(comm.get());

  lfi_client_close(in_comm->m_comm);

  debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_comm_disconnect] << End");
}

void fabric_server_control_comm::disconnect ( int id )
{
  XPN_PROFILE_FUNCTION();
  debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_comm_disconnect] >> Begin");
  
  lfi_client_close(id);

  debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_comm_disconnect] << End");
}


int64_t fabric_server_control_comm::read_operation ( xpn_server_msg &msg, int &rank_client_id, int &tag_client_id )
{
  XPN_PROFILE_FUNCTION();
  int ret = 0;
  debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_control_comm_read_operation] >> Begin");

  if (!shm_request){
    shm_request = {lfi_request_create(LFI_ANY_COMM_SHM), lfi_request_free};
    if (!shm_request){
        print("Error shm_request is null");
    }

    if (lfi_trecv_async(shm_request.get(), &shm_msg, sizeof(shm_msg), 0) < 0){
        print("Error in lfi_trecv_async")
        return -1;
    }
  }

  if (!peer_request){
    peer_request = {lfi_request_create(LFI_ANY_COMM_PEER), lfi_request_free};
    if (!peer_request){
        print("Error peer_request is null");
    }

    if (lfi_trecv_async(peer_request.get(), &peer_msg, sizeof(peer_msg), 0) < 0){
        print("Error in lfi_trecv_async")
        return -1;
    }
  }

  constexpr int REQUESTS_SIZE = 2;
  lfi_request *requests[REQUESTS_SIZE] = {shm_request.get(), peer_request.get()};

  debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_control_comm_read_operation] Before lfi_wait_any");
  int completed = lfi_wait_any(requests, REQUESTS_SIZE);

  debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_control_comm_read_operation] request shm  (RANK "<<lfi_request_source(shm_request.get())<<", TAG "<<shm_msg.tag<<")");
  debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_control_comm_read_operation] request peer (RANK "<<lfi_request_source(peer_request.get())<<", TAG "<<peer_msg.tag<<")");
  if (completed == 0){
    rank_client_id = lfi_request_source(shm_request.get());
    tag_client_id  = shm_msg.tag;
    ret = lfi_request_size(shm_request.get());
    std::memcpy(&msg, &shm_msg, shm_msg.get_size());

    // Reuse the request for a new recv
    if (lfi_trecv_async(shm_request.get(), &shm_msg, sizeof(shm_msg), 0) < 0){
        print("Error in lfi_trecv_async")
        return -1;
    }
  }else if (completed == 1){
    rank_client_id = lfi_request_source(peer_request.get());
    tag_client_id  = peer_msg.tag;
    ret = lfi_request_size(peer_request.get());
    std::memcpy(&msg, &peer_msg, peer_msg.get_size());

    // Reuse the request for a new recv
    if (lfi_trecv_async(peer_request.get(), &peer_msg, sizeof(peer_msg), 0) < 0){
        print("Error in lfi_trecv_async")
        return -1;
    }
  }else{
    return -1;
  }

  debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_control_comm_read_operation] read (RANK "<<rank_client_id<<", TAG "<<tag_client_id<<") = "<<ret);
  debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_control_comm_read_operation] << End");

  // Return OK
  return ret;
}

int64_t fabric_server_comm::read_operation ([[maybe_unused]] xpn_server_msg &msg,[[maybe_unused]] int &rank_client_id,[[maybe_unused]] int &tag_client_id )
{
  XPN_PROFILE_FUNCTION();
  int ret = 0;
  debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_comm_read_operation] >> Begin");

  // if (!shm_request){
  //   shm_request = {lfi_request_create(LFI_ANY_COMM_SHM), lfi_request_free};
  //   if (!shm_request){
  //       print("Error shm_request is null");
  //   }

  //   if (lfi_trecv_async(shm_request.get(), &shm_msg, sizeof(shm_msg), 0) < 0){
  //       print("Error in lfi_trecv_async")
  //       return -1;
  //   }
  // }

  // if (!peer_request){
  //   peer_request = {lfi_request_create(LFI_ANY_COMM_PEER), lfi_request_free};
  //   if (!peer_request){
  //       print("Error peer_request is null");
  //   }

  //   if (lfi_trecv_async(peer_request.get(), &peer_msg, sizeof(peer_msg), 0) < 0){
  //       print("Error in lfi_trecv_async")
  //       return -1;
  //   }
  // }

  // lfi_request *requests[2] = {shm_request.get(), peer_request.get()};

  // int completed = lfi_wait_any(requests, 2);

  // debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_comm_read_operation] request shm  (RANK "<<lfi_request_source(shm_request.get())<<", TAG "<<shm_msg.tag<<")");
  // debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_comm_read_operation] request peer (RANK "<<lfi_request_source(peer_request.get())<<", TAG "<<peer_msg.tag<<")");
  // if (completed == 0){
  //   rank_client_id = lfi_request_source(shm_request.get());
  //   tag_client_id  = shm_msg.tag;
  //   ret = lfi_request_size(shm_request.get());
  //   std::memcpy(&msg, &shm_msg, shm_msg.get_size());

  //   // shm_msg = {};

  //   // Reuse the request for a new recv
  //   if (lfi_trecv_async(shm_request.get(), &shm_msg, sizeof(shm_msg), 0) < 0){
  //       print("Error in lfi_trecv_async")
  //       return -1;
  //   }
  // }else if (completed == 1){
  //   rank_client_id = lfi_request_source(peer_request.get());
  //   tag_client_id  = peer_msg.tag;
  //   ret = lfi_request_size(peer_request.get());
  //   std::memcpy(&msg, &peer_msg, peer_msg.get_size());

  //   // peer_msg = {};

  //   // Reuse the request for a new recv
  //   if (lfi_trecv_async(peer_request.get(), &peer_msg, sizeof(peer_msg), 0) < 0){
  //       print("Error in lfi_trecv_async")
  //       return -1;
  //   }
  // }else{
  //   return -1;
  // }

  // debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_comm_read_operation] read (RANK "<<rank_client_id<<", TAG "<<tag_client_id<<") = "<<ret);
  debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_comm_read_operation] << End");

  // Return OK
  return ret;
}

int64_t fabric_server_comm::read_data ( void *data, int64_t size, int rank_client_id, int tag_client_id )
{
  XPN_PROFILE_FUNCTION_ARGS(size);
  int64_t ret = 0;

  debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_comm_read_data] >> Begin");

  if (size == 0) {
    return  0;
  }
  if (size < 0)
  {
    print("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_comm_read_data] ERROR: size < 0");
    return  -1;
  }

  // Get message
  debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_comm_read_data] Read data tag "<< tag_client_id);
  ret = lfi_trecv(rank_client_id, data, size, tag_client_id);
  if (ret < 0) {
    debug_warning("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_comm_read_data] ERROR: read fails");
  }

  debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_comm_read_data] read (RANK "<<rank_client_id<<", TAG "<<tag_client_id<<") = "<<ret);
  debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_comm_read_data] << End");

  // Return bytes read
  return ret;
}

int64_t fabric_server_comm::write_data ( const void *data, int64_t size, int rank_client_id, int tag_client_id )
{
  XPN_PROFILE_FUNCTION_ARGS(size);
  int64_t ret = 0;

  debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_comm_write_data] >> Begin");

  if (size == 0) {
      return 0;
  }
  if (size < 0)
  {
    print("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_comm_write_data] ERROR: size < 0");
    return -1;
  }

  // Send message
  debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_comm_write_data] Write data tag "<< tag_client_id);

  ret = lfi_tsend(rank_client_id, data, size, tag_client_id);
  if (ret < 0) {
    debug_warning("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_comm_write_data] ERROR: lfi_tsend fails "<<lfi_strerror(ret));
  }

  debug_info("[Server="<<ns::get_host_name()<<"] [FABRIC_SERVER_COMM] [fabric_server_comm_write_data] "<<ret<<" << End");

  // Return bytes written
  return ret;
}

} // namespace XPN