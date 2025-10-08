
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

  /* ... Include / Inclusion ........................................... */

  #include <string>
  #include <memory>
  #include <stdlib.h>
  #include <stdio.h>
  #include <ctype.h>
  #include "base_cpp/workers.hpp"
  #include "xpn_filesystem/xpn_filesystem.hpp"

  /* ... Const / Const ................................................. */

namespace XPN
{
  enum class server_type {
    MPI,
    SCK,
    MQTT,
    FABRIC,
  };
  
  constexpr const int KB = 1024;
  constexpr const int MB = (KB*KB);
  constexpr const int GB = (KB*MB);
  constexpr const int MAX_BUFFER_SIZE = (1*MB);
  constexpr const int MAX_PORT_NAME = 1024;
  constexpr const int DEFAULT_XPN_SERVER_CONTROL_PORT = 3456;
  // 0 is for dynamic assigment
  constexpr const int DEFAULT_XPN_SERVER_COMM_PORT = 0;
  constexpr const int DEFAULT_XPN_SERVER_CONNECTIONLESS_PORT = 0;
  constexpr const int DEFAULT_XPN_SERVER_MQTT_QOS = 0;

  /* ... Data structures / Estructuras de datos ........................ */

  class xpn_server_params
  {
  public:
    // server identification
    int  size;
    int  rank;

    int srv_control_port;
    int srv_comm_port;
    int srv_connectionless_port;

    // server configuration
    std::string shutdown_file;
    std::string shutdown_hostlist;
    workers_mode thread_mode;
    server_type srv_type; 
    filesystem_mode fs_mode;

    int mqtt_qos;
    int await_stop;

    // server arguments
    int    argc;
    char **argv;

  public:
    xpn_server_params(int argc, char *argv[]);
    // Delete default constructors
    xpn_server_params() = delete;
    // Delete copy constructor
    xpn_server_params(const xpn_server_params&) = delete;
    // Delete copy assignment operator
    xpn_server_params& operator=(const xpn_server_params&) = delete;
    // Delete move constructor
    xpn_server_params(xpn_server_params&&) = delete;
    // Delete move assignment operator
    xpn_server_params& operator=(xpn_server_params&&) = delete;

    void show_usage();
    void show();
    bool have_threads() { return static_cast<int>(thread_mode) > 0; }
    int get_argc() { return argc; }
    char** get_argv() { return argv; }
  };
  
  /* ................................................................... */

}