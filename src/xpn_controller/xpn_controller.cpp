
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

#include "xpn_controller.hpp"

namespace XPN {

int xpn_controller::start() {
    action current_action = action::NONE;
    debug_info("[XPN_CONTROLLER] >> Start");
    std::string_view act = m_args.get_arg(0);

    for (auto& [key, value] : actions_str) {
        if (act == key) {
            current_action = value;
        }
    }

    if (current_action == action::NONE) {
        std::cerr << "Error: unknown action '" << act << "'" << std::endl;
        return -1;
    }

    switch (current_action) {
        // Local execution
        case action::START:
            return run();
        case action::MK_CONFIG:
            return local_mk_config();
        // Remote execution
        default:
            return send_action(current_action);
    }
    debug_info("[XPN_CONTROLLER] >> End");
    return 0;
}
}  // namespace XPN

int main(int argc, char* argv[]) {
    XPN::xpn_controller controller(argc, argv);

    if (controller.m_args.parse() < 0) {
        exit(EXIT_FAILURE);
    }

    if (controller.start() < 0) {
        controller.m_args.help();
        exit(EXIT_FAILURE);
    }

    return 0;
}