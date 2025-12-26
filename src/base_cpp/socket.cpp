
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

#include "socket.hpp"
#include "base_cpp/fixed_string.hpp"
#include "base_cpp/xpn_env.hpp"
#include "base_cpp/debug.hpp"
#include "base_cpp/filesystem.hpp"

#include <string>
#include <iostream>
#include <netdb.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <string.h>
#include <unistd.h>
#include <thread>
#include <algorithm>

namespace XPN
{
    int64_t socket::send ( int socket, const void * buffer, uint64_t size )
    {
        int64_t ret;

        ret = filesystem::write(socket, buffer, size);
        if (ret < 0){
            debug_error("[SOCKET] [socket::recv] ERROR: socket write buffer size "<<size<<" "<<strerror(errno));
        }
        
        return size;
    }

    int64_t socket::recv ( int socket, void * buffer, uint64_t size )
    {
        int64_t ret;

        ret = filesystem::read(socket, buffer, size);
        if (ret < 0){
            debug_error("[SOCKET] [socket::recv] ERROR: socket read buffer size "<<size<<" "<<strerror(errno));
        }

        return size;
    }
    
    int64_t socket::send_line ( int socket, const char *buffer )
    {
        return socket::send(socket, buffer, strlen(buffer)+1);
    }

    int64_t socket::recv_line ( int socket, char *buffer, uint64_t n )
    {
        int64_t numRead;  /* bytes read in the last read() */
        uint64_t totRead;   /* total bytes read so far */
        char *buf;
        char ch;

        if (n <= 0 || buffer == NULL) {
            errno = EINVAL;
            return -1;
        }

        buf = buffer;
        totRead = 0;

        while (1)
        {
            numRead = socket::recv(socket, &ch, 1);  /* read one byte */

            if (numRead == -1) {
                if (errno == EINTR)      /* interrupted -> restart read() */
                    continue;
                else return -1;          /* another type of error */
            } else if (numRead == 0) {   /* EOF */
                if (totRead == 0)        /* no bytes read -> return 0 */
                    return 0;
                else break;
            } else {                     /* numRead must be 1 here */
                if (ch == '\n') break;
                if (ch == '\0') break;
                if (totRead < n - 1) {   /* discard > (n-1) bytes */
                    totRead++;
                    *buf++ = ch;
                }
            }
        }

        *buf = '\0';
        return totRead;
    }

    int64_t socket::send_str ( int socket, const std::string& str )
    {
        return socket::send_str(socket, std::string_view(str));
    }

    int64_t socket::send_str ( int socket, std::string_view str )
    {
        int64_t ret;
        uint64_t size_str = str.size();
        debug_info("Send_str size "<<size_str);
        ret = socket::send(socket, &size_str, sizeof(size_str));
        if (ret != sizeof(size_str)){
            print_error("send size of string");
            return ret;
        }
        if (size_str == 0){
            return 0;
        }
        debug_info("Send_str "<<std::string_view(str.data(), std::min(str.size(), static_cast<size_t>(128))));
        ret = socket::send(socket, &str[0], size_str);
        if (ret != static_cast<int64_t>(size_str)){
            print_error("send string");
            return ret;
        }
        return ret;
    }

    int64_t socket::recv_str ( int socket, std::string& str )
    {
        int64_t ret;
        uint64_t size_str = 0;
        ret = socket::recv(socket, &size_str, sizeof(size_str));
        if (ret != sizeof(size_str)){
            print_error("send size of string");
            return ret;
        }
        debug_info("Recv_str size "<<size_str);
        if (size_str == 0){
            return 0;
        }
        str.clear();
        str.resize(size_str, '\0');
        ret = socket::recv(socket, &str[0], size_str);
        if (ret != static_cast<int64_t>(size_str)){
            print_error("send string");
            return ret;
        }
        debug_info("Recv_str "<<std::string_view(str.data(), std::min(str.size(), static_cast<size_t>(128))));
        return ret;
    }

    int socket::socket_create(int socket_domain) {
        int ret = 0;
        int server_socket = ::socket(socket_domain, SOCK_STREAM, IPPROTO_TCP);
        if (server_socket < 0) {
            debug_error("[SOCKET] [socket::server_create] ERROR: socket fails");
            return -1;
        }

        debug_info("[SOCKET] [socket::server_create] Socket quit ipv6 only");
        int val = 0;
        if (socket_domain == AF_INET6){
            ret = setsockopt(server_socket, IPPROTO_IPV6, IPV6_V6ONLY, &val, sizeof(val));
            if (ret < 0) {
                debug_error("[SOCKET] [socket::server_create] ERROR: setsockopt fails");
                return -1;
            }
        }

        val = 1;
        ret = setsockopt(server_socket, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val));
        if (ret < 0) {
            debug_error("[SOCKET] [socket::server_create] ERROR: setsockopt fails");
            return -1;
        }

        debug_info("[SOCKET] [socket::server_create] Socket reuseaddr");

        val = 1;
        ret = setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&val, sizeof(int));
        if (ret < 0) {
            debug_error("[SOCKET] [socket::server_create] ERROR: setsockopt fails");
            return -1;
        }
        return server_socket;
    }

    int socket::server_create ( int port, int &out_socket )
    {
        int ret = 0;

        int server_socket = socket_create();
        if (server_socket < 0) {
            debug_error("[SOCKET] [socket::server_create] ERROR: socket fails");
            return -1;
        }

        // bind
        debug_info("[SOCKET] [socket::server_create] Socket bind port "<<port);

        struct sockaddr_in6 server_addr;
        memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin6_family      = AF_INET6;
        server_addr.sin6_addr        = in6addr_any;
        server_addr.sin6_port        = htons(port);

        ret = bind(server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr));
        if (ret < 0)
        {
            debug_error("[SOCKET] [socket::server_create] ERROR: bind fails");
            return -1;
        }

        // listen
        debug_info("[SOCKET] [socket::server_create] Socket listen");

        ret = listen(server_socket, SOMAXCONN);
        if (ret < 0)
        {
            debug_error("[SOCKET] [socket::server_create] ERROR: listen fails");
            return -1;
        }
        out_socket = server_socket;
        return 0;
    }

    int socket::server_port( int socket ) {
        struct sockaddr_storage server_addr;
        socklen_t len = sizeof(server_addr);
        int ret = ::getsockname(socket, (struct sockaddr *)&server_addr, &len);
        if (ret < 0) {
            debug_error("[SOCKET] [socket::server_port] ERROR: getsockname fails");
            return -1;
        }
        
        if (server_addr.ss_family == AF_INET) {
            const sockaddr_in* sin = reinterpret_cast<const sockaddr_in*>(&server_addr);
            ret = ntohs(sin->sin_port);
        } else if (server_addr.ss_family == AF_INET6) {
            const sockaddr_in6* sin6 = reinterpret_cast<const sockaddr_in6*>(&server_addr);
            ret = ntohs(sin6->sin6_port);
        } else {
            debug_error("[SOCKET] [socket::server_port] ERROR: socket server_addres with family not supported");
            return -1;
        }
        debug_info("[SOCKET] [socket::server_port] port "<<ret);
        return ret;
    }

    int socket::server_accept ( int socket, int &out_conection_socket )
    {
        struct sockaddr_storage client_addr;
        socklen_t sock_size = sizeof(client_addr);
        int new_socket = accept(socket, (struct sockaddr*)&client_addr, &sock_size);
        if (new_socket < 0) {
            debug_error("[SOCKET] [socket::accept_send] ERROR: socket accept");
            return -1;
        }
        out_conection_socket = new_socket;
        return 0;
    }

    [[maybe_unused]] static void print_sockaddr(const sockaddr* sa) {
        char host_str[NI_MAXHOST];

        if (sa == nullptr) {
            std::cerr << "sockaddr is null" << std::endl;
            return;
        }

        if (sa->sa_family == AF_INET) {
            const sockaddr_in* sin = reinterpret_cast<const sockaddr_in*>(sa);
            inet_ntop(AF_INET, &sin->sin_addr, host_str, NI_MAXHOST);
            int port = ntohs(sin->sin_port);
            std::cerr << "IPv4 Address: " << host_str << ", Port: " << port << std::endl;

        } else if (sa->sa_family == AF_INET6) {
            const sockaddr_in6* sin6 = reinterpret_cast<const sockaddr_in6*>(sa);
            inet_ntop(AF_INET6, &sin6->sin6_addr, host_str, NI_MAXHOST);
            int port = ntohs(sin6->sin6_port);
            std::cerr << "IPv6 Address: " << host_str << ", Port: " << port << std::endl;
        } else {
            std::cerr << "Direction family not supported" << std::endl;
        }
    }

    int resolve_hostname(const char *srv_name, int port, sockaddr_storage* pAddr)
    {
        int ret;
        addrinfo* pResultList = NULL;
        addrinfo hints = {};
        int result = -1;

        hints.ai_socktype = SOCK_STREAM;
        hints.ai_protocol = IPPROTO_TCP;
        char port_str[10] = {};
        std::snprintf(port_str, 9, "%d", port);
        ret = ::getaddrinfo(srv_name, port_str, &hints, &pResultList);
        debug_info("getaddrinfo("<<srv_name<<") = "<<ret<<(ret < 0 ? gai_strerror(ret) : ""));

        result = (ret == 0) ? 1 : -1;
        if (result != -1)
        {
            // just pick the first one found
            memcpy(pAddr, pResultList->ai_addr, pResultList->ai_addrlen);
            #ifdef DEBUG
            print_sockaddr(pResultList->ai_addr);
            #endif
            result = 0;
        }

        if (pResultList != NULL)
        {
            ::freeaddrinfo(pResultList);
        }

        return result;
    }

    int socket::client_connect ( std::string_view srv_name, int port, int &out_socket )
    {
        int client_fd;
        struct sockaddr_storage serv_addr;
        FixedString<HOST_NAME_MAX> name(srv_name);
        int ret = resolve_hostname(name.c_str(), port, &serv_addr);
        if (ret < 0) {
            debug_error("[SOCKET] [socket::client_connect] ERROR: resolve_hostname");
            return -1;
        }
        client_fd = socket_create(serv_addr.ss_family);
        if (client_fd < 0) 
        {
            debug_error("[SOCKET] [socket::client_connect] ERROR: socket creation error");
            return -1;
        }
        debug_info("srv_name "<<srv_name<<" port "<<port);
        #ifdef DEBUG
        print_sockaddr((struct sockaddr*)&serv_addr);
        #endif
        int status = connect(client_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
        if (status < 0) 
        {
            debug_error("[SOCKET] [socket::client_connect] ERROR: socket connection failed to "<<srv_name<<" in port "<<port<< " "<<errno<<" " <<strerror(errno));
            close(client_fd);
            return -1;
        }

        out_socket = client_fd;
        return 0;
    }

    int socket::client_connect ( std::string_view srv_name, int port, int timeout_ms, int &out_socket, int time_to_sleep_ms )
    {
        int ret = -1;
        auto start = std::chrono::high_resolution_clock::now();
        while (ret < 0) {
            debug_info("Try to connect to " << srv_name << " server");
            ret = socket::client_connect(srv_name, port, out_socket);
            if (ret < 0) {
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::high_resolution_clock::now() - start)
                                    .count();
                debug_error("Failed to connect to " << srv_name << " server. Elapsed time " << elapsed << " ms");
                if (elapsed > timeout_ms) {
                    debug_error("Socket connection " << srv_name);
                    return ret;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(time_to_sleep_ms));
            }
        }
        return ret;
    }

    int socket::close ( int socket )
    {
        int ret;

        ret = ::shutdown(socket, SHUT_RDWR);
        if (ret < 0) {
            debug_error("[SOCKET] [socket::socket_close] ERROR: socket shutdown "<<strerror(errno));
            return -1;
        }
        ret = ::close(socket);
        if (ret < 0) {
            debug_error("[SOCKET] [socket::socket_close] ERROR: socket close  "<<strerror(errno));
            return -1;
        }

        return ret;
    }
} // namespace XPN
