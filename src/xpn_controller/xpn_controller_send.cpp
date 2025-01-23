
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

#include <limits.h>
#include <unistd.h>

#include <fstream>
#include <iostream>

#include "base_cpp/subprocess.hpp"
#include "base_cpp/workers.hpp"
#include "nfi/nfi_server.hpp"
#include "xpn/xpn_conf.hpp"
#include "xpn_controller.hpp"

namespace XPN {

int xpn_controller::send_action(action act) {
    debug_info("[XPN_CONTROLlER] >> Start");
    xpn_conf conf;
    // TODO: do for more than the first partition
    const std::string &url = conf.partitions[0].controler_url;
    int ret;
    int socket;
    ret = socket::client_connect(url, socket::xpn_controller::DEFAULT_XPN_CONTROLLER_SCK_PORT, socket);
    if (ret < 0) {
        print_error("connecting to the xpn_controller");
        return -1;
    }
    int buf = socket::xpn_controller::ACTION_CODE;
    ret = socket::send(socket, &buf, sizeof(buf));
    if (ret != sizeof(buf)) {
        print_error("sending code to the xpn_controller");
        socket::close(socket);
        return -1;
    }
    int code = static_cast<int>(act);
    ret = socket::send(socket, &code, sizeof(code));
    if (ret != sizeof(code)) {
        print_error("sending code to the xpn_controller");
        socket::close(socket);
        return -1;
    }

    switch (act) {
        case action::STOP:
            ret = send_stop(socket);
            break;
        case action::START_SERVERS:
            ret = send_start_servers(socket);
            break;
        case action::STOP_SERVERS:
            ret = send_stop_servers(socket);
            break;
        default:
            break;
    }

    if (ret < 0) {
        print_error("send action");
    }

    ret = socket::recv(socket, &code, sizeof(code));
    if (ret != sizeof(code)) {
        print_error("sending code to the xpn_controller");
        socket::close(socket);
        return -1;
    }
    socket::close(socket);
    debug_info("[XPN_CONTROLlER] >> End");
    return code;
}

int xpn_controller::send_stop(int socket) {
    debug_info("[XPN_CONTROLlER] >> Start");
    int ret = send_stop_servers(socket);
    debug_info("[XPN_CONTROLlER] >> End");
    return ret;
}

int xpn_controller::send_mk_config(int socket) {
    debug_info("[XPN_CONTROLlER] >> Start");
    // Necesary options
    auto hostfile = m_args.get_option(option_hostfile);
    if (hostfile.empty()) {
        std::cerr << "To mk_config is necesary the option hostfile" << std::endl;
        exit(EXIT_FAILURE);
    }
    auto conffile = xpn_env::get_instance().xpn_conf;
    if (conffile == nullptr) {
        std::cerr << "To mk_config is necesary to set the XPN_CONF to a shared file path" << std::endl;
        exit(EXIT_FAILURE);
    }

    auto bsize = m_args.get_option(option_bsize);
    auto replication_level = m_args.get_option(option_replication_level);
    auto server_type = m_args.get_option(option_server_type);
    auto storage_path = m_args.get_option(option_storage_path);
    int64_t ret;
    // Send the required strings
    ret = socket::send_str(socket, hostfile);
    if (ret < 0) {
        print_error("send_str hostfile");
        return ret;
    }
    ret = socket::send_str(socket, std::string_view(conffile));
    if (ret < 0) {
        print_error("send_str conffile");
        return ret;
    }
    // Send the optionals strings
    ret = socket::send_str(socket, bsize);
    if (ret < 0) {
        print_error("send_str bsize");
        return ret;
    }
    ret = socket::send_str(socket, replication_level);
    if (ret < 0) {
        print_error("send_str replication_level");
        return ret;
    }
    ret = socket::send_str(socket, server_type);
    if (ret < 0) {
        print_error("send_str server_type");
        return ret;
    }
    ret = socket::send_str(socket, storage_path);
    if (ret < 0) {
        print_error("send_str storage_path");
        return ret;
    }

    // Recv result
    int res;
    ret = socket::recv(socket, &res, sizeof(res));
    if (ret != sizeof(ret)) {
        print_error("recv result");
        return ret;
    }
    debug_info("[XPN_CONTROLlER] >> End");
    return res;
}

int xpn_controller::send_start_servers(int socket) {
    int ret;
    debug_info("[XPN_CONTROLlER] >> Start");
    bool await = m_args.has_option(option_await);
    ret = socket::send(socket, &await, sizeof(await));
    if (ret != sizeof(await)) {
        print_error("send await");
        return -1;
    }
    debug_info("[XPN_CONTROLlER] >> End");
    return 0;
}

int xpn_controller::send_stop_servers(int socket) {
    int ret;
    debug_info("[XPN_CONTROLlER] >> Start");
    bool await = m_args.has_option(option_await);
    ret = socket::send(socket, &await, sizeof(await));
    if (ret != sizeof(await)) {
        print_error("send await");
        return -1;
    }
    debug_info("[XPN_CONTROLlER] >> End");
    return 0;
}
}  // namespace XPN