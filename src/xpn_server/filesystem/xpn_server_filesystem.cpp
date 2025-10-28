
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

#include "xpn_server_filesystem.hpp"

#include <iostream>

#include "xpn_server_filesystem_disk.hpp"
#include "xpn_server_filesystem_xpn.hpp"

namespace XPN {

std::unique_ptr<xpn_server_filesystem> xpn_server_filesystem::Create(filesystem_mode mode) {
    std::unique_ptr<xpn_server_filesystem> ret = nullptr;
    switch (mode) {
        case filesystem_mode::disk: {
            ret = std::make_unique<xpn_server_filesystem_disk>();
            break;
        }
        case filesystem_mode::xpn: {
            ret = std::make_unique<xpn_server_filesystem_xpn>();
            break;
        }
    }
    if (ret) {
        ret->m_mode = mode;
    } else {
        std::cerr << "Error: filesystem mode '" << static_cast<int>(mode) << "' is not defined." << std::endl;
    }
    return ret;
}
}  // namespace XPN