
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
#include <string_view>
#include <netdb.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>

namespace XPN
{
    class socket
	{
    public:
        class xpn_controller {
        public:
            constexpr static const int DEFAULT_XPN_CONTROLLER_SCK_PORT = 34567;
            constexpr static const int COMMAND_CODE                   = 111;
            constexpr static const int ACTION_CODE                    = 222;
            constexpr static const int PROFILER_CODE                  = 333;
        };

        class xpn_server {
        public:
            constexpr static const int DEFAULT_XPN_SCK_PORT     = 3456;
            constexpr static const int ACCEPT_CODE              = 123;
            constexpr static const int CONNECTIONLESS_PORT_CODE = 124;
            constexpr static const int FINISH_CODE              = 666;
            constexpr static const int FINISH_CODE_AWAIT        = 667;
            constexpr static const int STATS_CODE               = 444;
            constexpr static const int STATS_wINDOW_CODE        = 445;
            constexpr static const int PING_CODE                = 333;
        };
    public:
        static int64_t send ( int socket, const void * buffer, uint64_t size );
        static int64_t recv ( int socket, void * buffer, uint64_t size );
        static int64_t send_line ( int socket, const char *buffer );
        static int64_t recv_line ( int socket, char *buffer, uint64_t n );
        static int64_t send_str ( int socket, const std::string& str );
        static int64_t send_str ( int socket, std::string_view str );
        static int64_t recv_str ( int socket, std::string& str );
        static int socket_create( int socket_domain = AF_INET6);
        static int server_create ( int port, int &out_socket );
        static int server_port ( int socket );
        static int server_accept ( int socket, int &out_conection_socket );
        static int client_connect ( std::string_view srv_name, int port, int &out_socket );
        static int client_connect ( std::string_view srv_name, int port, int timeout_ms, int &out_socket, int time_to_sleep_ms = 200 );
        static int close ( int socket );
	};
} // namespace XPN
