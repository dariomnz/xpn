
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

#include "xpn_filesystem.hpp"

#include <iostream>

#include "xpn_filesystem_disk.hpp"
#include "xpn_filesystem_xpn.hpp"

namespace XPN {

std::unique_ptr<xpn_filesystem> xpn_filesystem::Create(filesystem_mode mode) {
    switch (mode) {
        case filesystem_mode::disk:
            return std::make_unique<xpn_filesystem_disk>();
        case filesystem_mode::xpn:
            return std::make_unique<xpn_filesystem_xpn>();
    }
    std::cerr << "Error: filesystem mode '" << static_cast<int>(mode) << "' is not defined." << std::endl;
    return nullptr;
}
}  // namespace XPN