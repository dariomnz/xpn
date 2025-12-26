
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

#include "base_cpp/debug.hpp"
#include "base_cpp/ns.hpp"
#include "base_cpp/socket.hpp"
#include "nfi_server.hpp"
#include "xpn/xpn_api.hpp"
#include "nfi/nfi_xpn_server/nfi_xpn_server.hpp"
#include "nfi/nfi_local/nfi_local.hpp"

#include <charconv>
#include <iostream>
#include <csignal>

namespace XPN
{
    std::unique_ptr<nfi_server> nfi_server::Create(std::string_view url)
    {
        xpn_url server_url = xpn_parser::parse(url);
        if (url.find(server_protocols::file) == 0 ||
            (xpn_env::get_instance().xpn_locality == 1 && is_local_server(server_url.server))){
                return std::make_unique<nfi_local>(server_url);
            }
        if (url.find(server_protocols::mpi_server) == 0 ||
            url.find(server_protocols::sck_server) == 0 ||
            url.find(server_protocols::mqtt_server)  == 0 ||
            url.find(server_protocols::fabric_server) == 0 ){
                return std::make_unique<nfi_xpn_server>(server_url);
            }
        
        std::cerr << "Error: server protocol '"<< url << "' is not defined." << std::endl;
        return nullptr;
    }

    nfi_server::nfi_server(xpn_url srv_url) : m_url(srv_url.url)
    {
        xpn_url url = xpn_parser::parse(m_url);
        m_protocol = url.protocol;
        m_server = url.server;
        if (url.port.empty()) {
            m_server_port = DEFAULT_XPN_SERVER_CONTROL_PORT;
        }else{
            int port = 0;
            auto [ptr, ec] = std::from_chars(url.port.begin(), url.port.end(), port);
            if (ec != std::errc()) {
                m_server_port = DEFAULT_XPN_SERVER_CONTROL_PORT;
            }
            m_server_port = port;
        }
        m_path = url.path;
    }

    int nfi_server::init_comm()
    {
        XPN_DEBUG_BEGIN;
        int res = 0;
        // Init the comunication
        m_control_comm = nfi_xpn_server_control_comm::Create(m_protocol);
        m_control_comm_connectionless = nfi_xpn_server_control_comm::Create(server_protocols::sck_server);

        // Connect to the server
        m_comm = m_control_comm->control_connect(m_server, m_server_port);
        if(m_comm){
            XPN_DEBUG("Connected successfull to "<<m_server);
        }

        if (m_comm == nullptr){
            m_error = -1;
            res = -1;
        }

        if (!xpn_env::get_instance().xpn_connect){
            m_control_comm->disconnect(m_comm);
            m_comm = nullptr;

            int connection_socket = 0;
            char port_name[MAX_PORT_NAME] = {};
            int ret = socket::client_connect(m_server, m_server_port, xpn_env::get_instance().xpn_connect_timeout_ms, connection_socket);
            if (ret < 0) {
                debug_error("[NFI_SERVER] [init_comm] ERROR: socket connect\n");
                return -1;
            }
            int buffer = socket::xpn_server::CONNECTIONLESS_PORT_CODE;
            ret = socket::send(connection_socket, &buffer, sizeof(buffer));
            if (ret < 0)
            {
                debug_error("[NFI_SERVER] [init_comm] ERROR: socket send\n");
                socket::close(connection_socket);
                return -1;
            }
            ret = socket::recv(connection_socket, port_name, MAX_PORT_NAME);
            if (ret < 0)
            {
                debug_error("[NFI_SERVER] [init_comm] ERROR: socket read\n");
                socket::close(connection_socket);
                return -1;
            }
            socket::close(connection_socket);

            m_connectionless_port = port_name;
            debug_info("[NFI_SERVER] [init_comm] connection less port "<<m_connectionless_port);

            if (m_connectionless_port.empty()) {
                print("Error: to use without XPN_SESSION_CONNECT xpn_server needs to be build with SCK_SERVER support")
                std::raise(SIGTERM);
            }
        }

        XPN_DEBUG_END;
        return res;
    }

    int nfi_server::destroy_comm()
    {
        XPN_DEBUG_BEGIN;
        int res = 0;
        if (m_comm != nullptr){
            m_control_comm->disconnect(m_comm);
        }

        m_control_comm.reset();

        XPN_DEBUG_END;
        return res;
    }

    bool nfi_server::is_local_server(std::string_view server)
    {
        return (server == ns::get_host_name() ||
                server == ns::get_host_ip() ||
                server == "localhost");
    }
} // namespace XPN
