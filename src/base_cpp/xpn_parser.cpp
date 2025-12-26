
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

#include "xpn_parser.hpp"

#include <csignal>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include "base_cpp/debug.hpp"
#include "base_cpp/xpn_conf.hpp"
#include "base_cpp/xpn_env.hpp"

namespace XPN {

xpn_url xpn_parser::parse(std::string_view url) {
    XPN_DEBUG_BEGIN;
    int res = 0;
    xpn_url result;
    result.url = url;
    // Find the position of "://"
    uint64_t protocol_pos = url.find("://");
    if (protocol_pos == std::string_view::npos) {
        std::cerr << "Invalid format of server_url: '://' not found '" << url << "'" << std::endl;
    } else {
        // Extract the first part (before "://")
        result.protocol = url.substr(0, protocol_pos);

        // Extract the second part (after "://")
        std::string_view remainder = url.substr(protocol_pos + 3);

        // Find the position of the first ':'
        uint64_t port_pos = remainder.find_last_of(':');
        if (port_pos == std::string_view::npos) {
            result.port = "";

            // Find the position of the first '/'
            uint64_t ip_pos = remainder.find('/');
            if (ip_pos == std::string_view::npos) {
                std::cerr << "Invalid format: '/' not found after IP '" << url << "'" << std::endl;
            } else {
                // Extract the IP address
                result.server = remainder.substr(0, ip_pos);
                // Extract the path (after the first '/')
                result.path = remainder.substr(ip_pos);
            }
        } else {
            // Extract the IP address
            result.server = remainder.substr(0, port_pos);
            remainder = remainder.substr(port_pos + 1);

            // Find the position of the first '/'
            uint64_t ip_pos = remainder.find('/');
            if (ip_pos == std::string_view::npos) {
                std::cerr << "Invalid format: '/' not found after IP '" << url << "'" << std::endl;
            } else {
                // Extract the IP address
                result.port = remainder.substr(0, ip_pos);
                // Extract the path (after the first '/')
                result.path = remainder.substr(ip_pos);
            }
        }
    }

    XPN_DEBUG("Parse '" << url << "' to protocol '" << result.protocol << "' server '" << result.server << "' port '"
                        << result.port << "' path '" << result.path << "'");
    XPN_DEBUG_END;
    return result;
}

std::string xpn_parser::create(xpn_url url) {
    std::stringstream ss;
    ss << url.protocol << "://" << url.server;
    if (!url.port.empty()) {
        ss << ":" << url.port;
    }
    ss << "/" << url.path;
    return ss.str();
}
}  // namespace XPN
