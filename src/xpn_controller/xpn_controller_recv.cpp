
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

#include <algorithm>
#include <fstream>
#include <iostream>

#include "base_cpp/subprocess.hpp"
#include "base_cpp/workers.hpp"
#include "base_cpp/xpn_conf.hpp"
#include "nfi/nfi_server.hpp"
#include "xpn_controller.hpp"

namespace XPN {

int xpn_controller::recv_action(int socket, action &act) {
    int ret;
    int code;
    debug_info("[XPN_CONTROLLER] >> Start");
    ret = socket::recv(socket, &code, sizeof(code));
    if (ret != sizeof(code)) {
        print_error("recving code");
        socket::close(socket);
        return -1;
    }
    act = static_cast<action>(code);

    switch (act) {
        case action::STOP:
            code = recv_stop(socket);
            break;
        case action::START_SERVERS:
            code = recv_start_servers(socket);
            break;
        case action::STOP_SERVERS:
            code = recv_stop_servers(socket);
            break;
        case action::PING_SERVERS:
            code = recv_ping_servers(socket);
            break;
        case action::EXPAND_NEW:
            code = recv_expand_new(socket);
            break;
        case action::EXPAND_CHANGE:
            code = recv_expand_change(socket);
            break;
        case action::SHRINK_NEW:
            code = recv_shrink_new(socket);
            break;
        case action::SHRINK_CHANGE:
            code = recv_shrink_change(socket);
            break;
        default:
            code = -1;
            std::cerr << "Unknown action" << std::endl;
            break;
    }

    ret = socket::send(socket, &code, sizeof(code));
    if (ret != sizeof(code)) {
        print_error("sending code to the xpn_controller");
        socket::close(socket);
        return -1;
    }
    debug_info("[XPN_CONTROLLER] >> End");
    if (code < 0) {
        return code;
    }
    return 0;
}

int xpn_controller::recv_stop(int socket) {
    debug_info("[XPN_CONTROLLER] >> Start");
    // Stop the servers to stop the controler
    int ret = recv_stop_servers(socket);
    
    end_profiler();
    debug_info("[XPN_CONTROLLER] >> End");
    return ret;
}

int xpn_controller::recv_mk_config(int socket) {
    // Recv the data
    std::string hostfile, conffile, bsize, replication_level, server_type, storage_path;
    int64_t ret;
    debug_info("[XPN_CONTROLLER] >> Start");
    ret = socket::recv_str(socket, hostfile);
    if (ret < 0) {
        print_error("recv_str hostfile");
        return ret;
    }
    ret = socket::recv_str(socket, conffile);
    if (ret < 0) {
        print_error("recv_str conffile");
        return ret;
    }
    ret = socket::recv_str(socket, bsize);
    if (ret < 0) {
        print_error("recv_str bsize");
        return ret;
    }
    ret = socket::recv_str(socket, replication_level);
    if (ret < 0) {
        print_error("recv_str replication_level");
        return ret;
    }
    ret = socket::recv_str(socket, server_type);
    if (ret < 0) {
        print_error("recv_str server_type");
        return ret;
    }
    ret = socket::recv_str(socket, storage_path);
    if (ret < 0) {
        print_error("recv_str storage_path");
        return ret;
    }

    char hostname[HOST_NAME_MAX];
    ret = gethostname(hostname, HOST_NAME_MAX);
    if (ret < 0) {
        print_error("gethostname");
    }

    xpn_conf::partition part;

    part.controler_url = hostname;
    if (!bsize.empty()) {
        part.bsize = atoi(std::string(bsize).c_str());
    }
    if (!replication_level.empty()) {
        part.replication_level = atoi(std::string(replication_level).c_str());
    }

    // Read the hosts
    {
        std::string filename(hostfile);
        std::ifstream file(filename);
        if (!file.is_open()) {
            std::cerr << "Error: hostfile '" << filename << "' cannot be open " << std::endl;
            return -1;
        }
        std::string line;
        // Get the options or default op
        if (server_type.empty()) {
            server_type = XPN_CONF::DEFAULT_SERVER_TYPE;
        }
        if (storage_path.empty()) {
            storage_path = XPN_CONF::DEFAULT_STORAGE_PATH;
        }
        while (std::getline(file, line)) {
            if (!line.empty()) {
                line = xpn_parser::create(server_type + "_server", line, storage_path);
                part.server_urls.emplace_back(line);
            }
            line.clear();
        }
    }

    {
        std::string filename(conffile);
        std::ofstream file(filename, std::ios::trunc);
        if (!file.is_open()) {
            std::cerr << "Error: conffile '" << filename << "' cannot be open " << std::endl;
            return -1;
        }

        file << part.to_string();
    }
    debug_info("[XPN_CONTROLLER] >> End");

    return 0;
}

int xpn_controller::recv_start_servers(int socket) {
    bool await, debug;
    int ret;
    debug_info("[XPN_CONTROLLER] >> Start");
    ret = socket::recv(socket, &await, sizeof(await));
    if (ret != sizeof(await)) {
        print_error("recv await");
        return -1;
    }
    int server_cores = 0;
    ret = socket::recv(socket, &server_cores, sizeof(server_cores));
    if (ret != sizeof(server_cores)) {
        print_error("recv server_cores");
        return -1;
    }
    ret = socket::recv(socket, &debug, sizeof(debug));
    if (ret != sizeof(debug)) {
        print_error("recv debug");
        return -1;
    }
    ret = start_servers(await, server_cores, debug);
    debug_info("[XPN_CONTROLLER] >> End");
    return ret;
}

int xpn_controller::recv_stop_servers(int socket) {
    bool await;
    int ret;
    debug_info("[XPN_CONTROLLER] >> Start");
    ret = socket::recv(socket, &await, sizeof(await));
    if (ret != sizeof(await)) {
        print_error("recv await");
        return -1;
    }
    ret = stop_servers(await);
    debug_info("[XPN_CONTROLLER] >> End");
    return ret;
}

int xpn_controller::recv_command(int socket) {
    int ret;
    std::string command;
    debug_info("[XPN_CONTROLLER] >> Start");
    ret = socket::recv_str(socket, command);
    if (ret < 0) {
        print_error("recv_str failed");
        return -1;
    }
    bool await;
    ret = socket::recv(socket, &await, sizeof(await));
    if (ret < 0) {
        print_error("Cannot recv await command");
        return -1;
    }
    debug_info("Recv command: '" << command << "'");
    subprocess::process proc(command);
    proc.set_wait_on_destroy(await);
    debug_info("launched command: '" << command << "'");
    int res = 0;
    if (await) {
        debug_info("Wait command: '" << command << "'");
        res = proc.wait_status();
    }

    ret = socket::send(socket, &res, sizeof(res));
    if (ret < 0) {
        print_error("Cannot send str_size");
        return -1;
    }
    debug_info("[XPN_CONTROLLER] >> End");
    return 0;
}

int xpn_controller::recv_ping_servers([[maybe_unused]] int socket) {
    int ret;
    debug_info("[XPN_CONTROLLER] >> Start");
    ret = ping_servers();
    debug_info("[XPN_CONTROLLER] >> End");
    return ret;
}

std::vector<std::string_view> split(const std::string_view str, const char delim = ' ') {
    std::vector<std::string_view> result;
    int num_delim = std::count(str.begin(), str.end(), delim);
    result.reserve(num_delim + 1);

    int left = 0;
    int right = -1;

    for (int i = 0; i < static_cast<int>(str.size()); i++) {
        if (str[i] == delim) {
            left = right;
            right = i;
            int index = left + 1;
            int length = right - index;

            result.emplace_back(std::string_view(str.begin() + index, length));
        }
    }
    result.emplace_back(std::string_view(str.begin() + right + 1, str.size() - right - 1));
    return result;
}

int xpn_controller::recv_expand_new(int socket) {
    // Recv the data
    std::string host_list;
    int64_t ret;
    debug_info("[XPN_CONTROLLER] >> Start");
    ret = socket::recv_str(socket, host_list);
    if (ret < 0) {
        print_error("recv_str hostfile");
        return ret;
    }

    std::vector<std::string_view> new_servers = split(host_list, ',');
    std::vector<std::string> old_servers;
    get_conf_servers(old_servers);

    std::vector<std::string_view> servers;
    servers.assign(old_servers.begin(), old_servers.end());
    // Remove the old servers to have the new servers append in to the end
    auto it = std::remove_if(new_servers.begin(), new_servers.end(), [&old_servers](std::string_view srv) {
        return std::find(old_servers.begin(), old_servers.end(), srv) != old_servers.end();
    });
    uint64_t num_erased = new_servers.end() - it;
    new_servers.erase(it, new_servers.end());

    if (num_erased != old_servers.size()) {
        std::cerr << "Error: expand_new there are '" << (old_servers.size() - num_erased)
                  << "' servers mising in new_servers" << std::endl;
        return -1;
    }

    for (auto &srv : new_servers) {
        servers.emplace_back(srv);
    }

    int res = expand(servers);

    debug_info("[XPN_CONTROLLER] >> End");
    return res;
}

int xpn_controller::recv_expand_change(int socket) {
    // Recv the data
    std::string host_list;
    int64_t ret;
    debug_info("[XPN_CONTROLLER] >> Start");
    ret = socket::recv_str(socket, host_list);
    if (ret < 0) {
        print_error("recv_str hostfile");
        return ret;
    }

    std::vector<std::string_view> change_servers = split(host_list, ',');
    std::vector<std::string> old_servers;
    get_conf_servers(old_servers);

    std::vector<std::string_view> servers;
    servers.assign(old_servers.begin(), old_servers.end());

    for (auto &srv : change_servers) {
        if (std::find(servers.begin(), servers.end(), srv) != servers.end()) {
            std::cerr << "Error: expand_change the serv '" << srv << "' is already in the conf" << std::endl;
            return -1;
        }
        servers.emplace_back(srv);
    }

    int res = expand(servers);

    debug_info("[XPN_CONTROLLER] >> End");
    return res;
}

int xpn_controller::recv_shrink_new(int socket) {
    // Recv the data
    std::string host_list;
    int64_t ret;
    debug_info("[XPN_CONTROLLER] >> Start");
    ret = socket::recv_str(socket, host_list);
    if (ret < 0) {
        print_error("recv_str hostfile");
        return ret;
    }

    std::vector<std::string_view> new_servers = split(host_list, ',');
    std::vector<std::string> old_servers;
    get_conf_servers(old_servers);

    // Check corrent servers
    bool error = false;
    for (auto &srv : new_servers) {
        if (std::find(old_servers.begin(), old_servers.end(), srv) == old_servers.end()) {
            std::cerr << "Error: shrink_new the server '" << srv
                      << "' does not exists in the current configuration servers" << std::endl;
            error = true;
        }
    }
    if (error) {
        return -1;
    }

    int res = shrink(new_servers);

    debug_info("[XPN_CONTROLLER] >> End");
    return res;
}

int xpn_controller::recv_shrink_change(int socket) {
    // Recv the data
    std::string host_list;
    int64_t ret;
    debug_info("[XPN_CONTROLLER] >> Start");
    ret = socket::recv_str(socket, host_list);
    if (ret < 0) {
        print_error("recv_str hostfile");
        return ret;
    }

    std::vector<std::string_view> to_remove_servers = split(host_list, ',');
    std::vector<std::string> old_servers;
    get_conf_servers(old_servers);

    std::vector<std::string_view> servers;
    servers.assign(old_servers.begin(), old_servers.end());
    // Remove the old servers to have the new servers append in to the end
    auto it = std::remove_if(servers.begin(), servers.end(), [&to_remove_servers](std::string_view srv) {
        return std::find(to_remove_servers.begin(), to_remove_servers.end(), srv) != to_remove_servers.end();
    });
    uint64_t num_erased = servers.end() - it;
    servers.erase(it, servers.end());

    if (num_erased != to_remove_servers.size()) {
        for (auto &srv : to_remove_servers) {
            if (std::find(old_servers.begin(), old_servers.end(), srv) == old_servers.end()) {
                std::cerr << "Error: shrink_change the server '" << srv
                          << "' does not exists in the current configuration servers" << std::endl;
            }
        }
        return -1;
    }

    int res = shrink(servers);

    debug_info("[XPN_CONTROLLER] >> End");
    return res;
}

int xpn_controller::recv_profiler(int socket) {
    std::string data;
    int64_t ret;
    debug_info("[XPN_CONTROLLER] >> Start");
    ret = socket::recv_str(socket, data);
    if (ret < 0) {
        print_error("recv_str data");
        return ret;
    }

    int code = -1;
    auto profiler_file = xpn_env::get_instance().xpn_profiler_file;
    if (profiler_file) {
        std::ofstream file(profiler_file, std::ios::app);
        if (file.is_open()) {
            debug_info("[XPN_CONTROLLER] writing " << data.size() << " to profile file " << profiler_file);
            file << data;
            code = 0;
        } else {
            print("Error: opening file " << profiler_file);
        }
    }

    ret = socket::send(socket, &code, sizeof(code));
    if (ret != sizeof(code)) {
        print_error("sending code to the profiler");
        return -1;
    }

    debug_info("[XPN_CONTROLLER] >> End");
    return 0;
}
}  // namespace XPN