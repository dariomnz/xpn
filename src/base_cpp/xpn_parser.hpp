
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

#include <sstream>
#include <string>
#include <vector>

namespace XPN {
class xpn_parser {
   public:
    xpn_parser(const std::string& url);
    static std::tuple<std::string_view, std::string_view, std::string_view, std::string_view> parse(const std::string& url);
    static std::string create(std::string_view protocol, std::string_view server, std::string_view path);

    const std::string m_url;
    std::string_view m_protocol;
    std::string_view m_server;
    std::string_view m_server_port;
    std::string_view m_path;
};
}  // namespace XPN