
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

#include <csignal>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include "xpn_parser.hpp"
#include "base_cpp/debug.hpp"
#include "base_cpp/xpn_conf.hpp"
#include "base_cpp/xpn_env.hpp"

namespace XPN {

xpn_parser::xpn_parser(const std::string& url) : m_url(url) {
    XPN_DEBUG_BEGIN;
    int res = 0;

    std::tie(m_protocol, m_server, m_server_port, m_path) = parse(m_url);

    if (m_protocol.empty()) {
        std::cerr << "Error cannot parse protocol of url '" << m_url << "'" << std::endl;
        std::raise(SIGTERM);
    }
    if (m_server.empty()) {
        std::cerr << "Error cannot parse server of url '" << m_url << "'" << std::endl;
        std::raise(SIGTERM);
    }
    if (m_path.empty()) {
        std::cerr << "Error cannot parse path of url '" << m_url << "'" << std::endl;
        std::raise(SIGTERM);
    }
    XPN_DEBUG_END;
}

std::tuple<std::string_view, std::string_view, std::string_view, std::string_view> xpn_parser::parse(const std::string& url) {
    XPN_DEBUG_BEGIN;
    int res = 0;
    std::string_view sv_url(url);
    std::string_view sv_protocol;
    std::string_view sv_server;
    std::string_view sv_server_port;
    std::string_view sv_path;
    // Find the position of "://"
    uint64_t protocol_pos = sv_url.find("://");
    if (protocol_pos == std::string_view::npos) {
        std::cerr << "Invalid format of server_url: '://' not found '" << url << "'" << std::endl;
    } else {
        // Extract the first part (before "://")
        sv_protocol = sv_url.substr(0, protocol_pos);

        // Extract the second part (after "://")
        std::string_view remainder = sv_url.substr(protocol_pos + 3);

        // Find the position of the first ':'
        uint64_t port_pos = remainder.find_last_of(':');
        if (port_pos == std::string_view::npos) {
            sv_server_port = "";

            // Find the position of the first '/'
            uint64_t ip_pos = remainder.find('/');
            if (ip_pos == std::string_view::npos) {
                std::cerr << "Invalid format: '/' not found after IP '" << url << "'" << std::endl;
            } else {
                // Extract the IP address
                sv_server = remainder.substr(0, ip_pos);
                // Extract the path (after the first '/')
                sv_path = remainder.substr(ip_pos);
            }
        }else{
            // Extract the IP address
            sv_server = remainder.substr(0, port_pos);
            remainder = remainder.substr(port_pos + 1);

            // Find the position of the first '/'
            uint64_t ip_pos = remainder.find('/');
            if (ip_pos == std::string_view::npos) {
                std::cerr << "Invalid format: '/' not found after IP '" << url << "'" << std::endl;
            } else {
                // Extract the IP address
                sv_server_port = remainder.substr(0, ip_pos);
                // Extract the path (after the first '/')
                sv_path = remainder.substr(ip_pos);
            }
        }
    }

    XPN_DEBUG("Parse '" << sv_url << "' to protocol '" << sv_protocol << "' server '" << sv_server << "' port '" << sv_server_port << "' path '" << sv_path << "'");
    XPN_DEBUG_END;
    return {sv_protocol, sv_server, sv_server_port, sv_path};
}

std::string xpn_parser::create(std::string_view protocol, std::string_view server, std::string_view path) {
    std::stringstream ss;
    ss << protocol << "://" << server << "/" << path;
    return ss.str();
}
}  // namespace XPN
