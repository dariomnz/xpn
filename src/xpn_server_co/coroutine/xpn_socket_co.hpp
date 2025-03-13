
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

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/tcp.h>

#include <string>
#include <string_view>

#include "xpn_coroutine.hpp"

namespace XPN {
class socket_co {
   public:
    static task<ssize_t> send(int socket, const void* buffer, size_t size) {
        int r;
        int l = size;
        const char* buff = static_cast<const char*>(buffer);
        debug_info(">> Begin");

        do {
            r = co_await SocketSendAwaitable(socket, buff, l);
            if (r <= 0) co_return r; /* fail */

            l = l - r;
            buff = buff + r;
        } while ((l > 0) && (r >= 0));

        debug_info(">> End = " << size);
        co_return size;
    }

    static task<ssize_t> recv(int socket, void* buffer, size_t size) {
        int r;
        int l = size;
        debug_info(">> Begin");
        char* buff = static_cast<char*>(buffer);

        do {
            r = co_await SocketRecvAwaitable(socket, buff, l);
            if (r <= 0) co_return r; /* fail */

            l = l - r;
            buff = buff + r;
        } while ((l > 0) && (r >= 0));

        debug_info(">> End = " << size);
        co_return size;
    }
};
}  // namespace XPN
