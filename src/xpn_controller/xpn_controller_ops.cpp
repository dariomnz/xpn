
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

#include <filesystem>
#include <fstream>
#include <iostream>

#include "base_cpp/args.hpp"
#include "base_cpp/fixed_task_queue.hpp"
#include "base_cpp/subprocess.hpp"
#include "base_cpp/workers.hpp"
#include "base_cpp/xpn_conf.hpp"
#include "nfi/nfi_server.hpp"
#include "xpn_controller.hpp"

namespace XPN {

std::string xpn_controller::usage() {
    std::stringstream out;
    out << "xpn_controller [OPTION]... [ACTION]" << std::endl;
    out << "Actions: " << std::endl;
    uint64_t max_str = 0;
    for (auto& [key, _] : actions_str) {
        if (max_str < key.size()) {
            max_str = key.size();
        }
    }
    for (auto& [key, act] : actions_str) {
        auto it = actions_str_help.find(act);
        if (it == actions_str_help.end()) {
            throw std::invalid_argument("This should not happend. There are no " + key + " in action_str_help");
        }
        out << "  " << key;
        out << std::string(max_str - key.size(), ' ');
        out << "    ";
        out << (*it).second;
        out << std::endl;
    }
    return out.str();
}

int xpn_controller::run() {
    int ret;
    debug_info("[XPN_CONTROLLER] >> Start");

    int server_socket = -1;
    ret = socket::server_create(xpn_env::get_instance().xpn_controller_sck_port, server_socket);
    if (ret < 0) {
        print_error("starting the socket");
        return -1;
    }

    int server_cores = ::atoi(std::string(m_args.get_option(option_server_cores)).c_str());
    ret = start_servers(m_args.has_option(option_await), server_cores, m_args.has_option(option_debug));
    if (ret < 0) {
        std::cerr << "Error in start_servers" << std::endl;
        socket::close(server_socket);
        return ret;
    }

    start_profiler();

    int connection_socket = -1;
    int op_code = 0;
    std::atomic<bool> running = true;
    std::mutex command_code_mutex;
    std::mutex action_code_mutex;
    std::mutex profiler_code_mutex;
    std::unique_ptr<workers> worker = workers::Create(workers_mode::thread_on_demand, false);

    while (running) {
        connection_socket = -1;
        debug_info("[XPN_CONTROLLER] Accepting connections in " << xpn_env::get_instance().xpn_controller_sck_port);
        ret = socket::server_accept(server_socket, connection_socket);
        if (!running) break;
        if (ret < 0 || connection_socket < 0) {
            print_error("accept failed");
            continue;
        }
        debug_info("[XPN_CONTROLLER] Connection accepted");

        ret = socket::recv(connection_socket, &op_code, sizeof(op_code));
        if (ret != sizeof(op_code)) {
            print_error("accept failed");
            socket::close(connection_socket);
            continue;
        }
        debug_info("[XPN_CONTROLLER] Recieve op " << op_code);

        switch (op_code) {
            case socket::xpn_controller::COMMAND_CODE:
                worker->launch_no_future([&, connection_socket]() {
                    std::unique_lock lock(command_code_mutex);
                    recv_command(connection_socket);
                    socket::close(connection_socket);
                });
                break;

            case socket::xpn_controller::ACTION_CODE:
                worker->launch_no_future([&, connection_socket]() {
                    std::unique_lock lock(action_code_mutex);
                    action act;
                    recv_action(connection_socket, act);
                    if (act == action::STOP) {
                        running = false;
                    }
                    socket::close(connection_socket);
                    socket::close(server_socket);
                });
                break;
            case socket::xpn_controller::PROFILER_CODE:
                worker->launch_no_future([&, connection_socket]() {
                    std::unique_lock lock(profiler_code_mutex);
                    recv_profiler(connection_socket);
                    socket::close(connection_socket);
                });
                break;

            default:
                print_error("Recv unknown op_code '" << op_code << "'");
                break;
        }
    }
    worker->wait_all();
    socket::close(server_socket);
    debug_info("[XPN_CONTROLLER] >> End");
    return 0;
}

int xpn_controller::local_mk_config() {
    debug_info("[XPN_CONTROLLER] >> Start");
    // Necesary options
    auto hostfile = m_args.get_option(option_hostfile);
    auto hostlist = m_args.get_option(option_hostlist);
    auto conffile = xpn_env::get_instance().xpn_conf;
    // Optional options
    auto bsize = m_args.get_option(option_bsize);
    auto replication_level = m_args.get_option(option_replication_level);
    auto server_type = m_args.get_option(option_server_type);
    auto storage_path = m_args.get_option(option_storage_path);

    int ret = mk_config(hostfile, hostlist, conffile, bsize, replication_level, server_type, storage_path);

    debug_info("[XPN_CONTROLLER] >> End");
    return ret;
}

// TODO: allow use of hostlist, not only hostfile
int xpn_controller::mk_config(std::string_view hostfile, [[maybe_unused]] std::string_view hostlist,
                              const char* conffile, std::string_view bsize,
                              std::string_view replication_level, std::string_view server_type,
                              std::string_view storage_path) {
    int ret;
    debug_info("[XPN_CONTROLLER] >> Start");
    if (hostfile.empty()) {
        std::cerr << "To mk_config is necesary the option hostfile" << std::endl;
        return -1;
    }
    if (conffile == nullptr) {
        std::cerr << "To mk_config is necesary to set the XPN_CONF to a shared file path" << std::endl;
        return -1;
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
        std::ifstream file(filename.c_str());
        if (!file.is_open()) {
            std::cerr << "Error: hostfile '" << filename << "' cannot be open " << std::endl;
            return -1;
        }
        std::string line;
        // Get the options or default op
        std::string_view server_type_str = server_type.empty() ? XPN_CONF::DEFAULT_SERVER_TYPE : server_type;
        std::string_view storage_path_str =
            storage_path.empty() ? XPN_CONF::DEFAULT_STORAGE_PATH : storage_path;
        while (std::getline(file, line)) {
            if (!line.empty()) {
                FixedString<64> protocol = server_type_str;
                protocol += "_server";
                xpn_url url{.protocol=protocol, .server=line, .path=storage_path_str};
                line = xpn_parser::create(url);
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

int xpn_controller::update_config(const std::vector<std::string_view>& new_hostlist) {
    auto conffile = xpn_env::get_instance().xpn_conf;
    if (conffile == nullptr) {
        std::cerr << "To update_config is necesary to set the XPN_CONF to a shared file path" << std::endl;
        return -1;
    }

    xpn_conf conf;
    // TODO: doit for more than the first partition;
    xpn_conf::partition part = conf.partitions[0];

    xpn_url url = xpn_parser::parse(conf.partitions[0].server_urls[0]);

    part.server_urls.clear();
    part.server_urls.reserve(new_hostlist.size());
    for (auto& srv : new_hostlist) {
        xpn_url srv_url{.protocol=url.protocol, .server=srv, .path=url.path};
        part.server_urls.emplace_back(xpn_parser::create(srv_url));
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
    return 0;
}

int xpn_controller::start_servers(bool await, int server_cores, bool debug) {
    int ret = 0;
    std::vector<std::string> servers;

    debug_info("[XPN_CONTROLLER] >> Start");
    xpn_conf conf;
    if (get_conf_servers(servers) < 0) {
        std::cerr << "Cannot get conf servers" << std::endl;
        return -1;
    }
    if (servers.size() == 0) {
        std::cerr << "The servers in the conf are 0" << std::endl;
        return -1;
    }

    // Save for future starts
    if (server_cores != 0) {
        m_server_cores = server_cores;
    }

    std::string command;
    std::vector<std::string> args;
    if (subprocess::have_command("srun")) {
        command = "srun";
        args.emplace_back("-n");
        args.emplace_back(std::to_string(servers.size()));
        args.emplace_back("-N");
        args.emplace_back(std::to_string(servers.size()));
        args.emplace_back("-l");
#ifdef DEBUG
        args.emplace_back("-v");
#endif
        args.emplace_back("--cpus-per-task");
        if (server_cores == 0) {
            args.emplace_back(std::to_string(std::thread::hardware_concurrency()));

            args.emplace_back("--overcommit");
            args.emplace_back("--overlap");
            args.emplace_back("--oversubscribe");
            args.emplace_back("--cpu-bind=no");
        } else {
            args.emplace_back(std::to_string(server_cores));
        }
        args.emplace_back("--export=ALL");
        args.emplace_back("--mpi=none");
        std::stringstream servers_list;
        for (uint64_t i = 0; i < servers.size(); i++) {
            servers_list << servers[0];
            if (i != (servers.size() - 1)) {
                servers_list << ",";
            }
            m_servers.emplace(servers[i]);
        }
        args.emplace_back("-w");
        args.emplace_back(servers_list.str());
        if (debug) {
            args.emplace_back("gdb");
            args.emplace_back("-batch");
            args.emplace_back("-ex");
            args.emplace_back("run");
            args.emplace_back("-ex");
            args.emplace_back("bt");
            args.emplace_back("--args");
        }
        args.emplace_back("xpn_server");
        args.emplace_back("-s");
        xpn_url url = xpn_parser::parse(conf.partitions[0].server_urls[0]);
        uint64_t pos = url.protocol.find('_');
        args.emplace_back(url.protocol.substr(0, pos));
        args.emplace_back("-t");
        args.emplace_back("pool");
    } else {
        // TODO: with mpiexec
        throw std::runtime_error("TODO: launch with mpiexec the servers");
    }
#ifdef DEBUG
    std::stringstream command_str;
    command_str << command << " ";
    for (auto& arg : args) {
        command_str << arg << " ";
    }
    debug_info("Command to launch: '" << command_str.str() << "'");
#endif

    m_servers_process.execute(command, args, false);
    if (!m_servers_process.is_running()) {
        std::cerr << "The process to launch the servers is not running" << std::endl;
        return -1;
    }

    if (await) {
        ret = ping_servers();
        if (ret < 0) {
            std::cerr << "Error in ping the servers" << std::endl;
        }
    }
    debug_info("[XPN_CONTROLLER] >> End");
    return ret;
}

int xpn_controller::stop_servers(bool await) {
    int res = 0;
    debug_info("[XPN_CONTROLLER] >> Start");
    std::unique_ptr<workers> worker = workers::Create(workers_mode::thread_on_demand);
    
    auto result_handler = [&](const WorkerResult& r) {
        if (r.result < 0) {
            res = r.result;
            errno = r.errorno;
        }
        return true; // Continue
    };
    FixedTaskQueue tasks(*worker, result_handler);
#ifdef DEBUG
    for (auto& srv : m_servers) {
        debug_info("Server to stop: " << srv);
    }
#endif

    for (auto& name : m_servers) {
        tasks.launch([&name, await]() {
            debug_info("Stopping server (" << name << ")");
            int socket;
            int ret;
            int buffer = socket::xpn_server::FINISH_CODE;
            if (await) {
                buffer = socket::xpn_server::FINISH_CODE_AWAIT;
            }
            // TODO: rethink with different ports
            ret = socket::client_connect(name, DEFAULT_XPN_SERVER_CONTROL_PORT, socket);
            if (ret < 0) {
                print("[XPN_CONTROLLER] ERROR: socket connection " << name);
                return WorkerResult(ret);
            }

            ret = socket::send(socket, &buffer, sizeof(buffer));
            if (ret < 0) {
                print("[XPN_CONTROLLER] ERROR: socket send " << name);
            }

            if (!await) {
                socket::close(socket);
            }

            if (await) {
                ret = socket::recv(socket, &buffer, sizeof(buffer));
                if (ret < 0) {
                    print("[XPN_CONTROLLER] ERROR: socket recv " << name);
                }
                socket::close(socket);
            }
            return WorkerResult(ret);
        });
    }

    tasks.wait_remaining();

    m_servers.clear();

    if (await) {
        m_servers_process.wait_status();
    }

    debug_info("[XPN_CONTROLLER] >> End");

    return res;
}

int xpn_controller::ping_servers() {
    int res = 0;
    debug_info("[XPN_CONTROLLER] >> Start");
    std::unique_ptr<workers> worker = workers::Create(workers_mode::thread_on_demand);

    auto result_handler = [&](const WorkerResult& r) {
        if (r.result < 0) {
            res = r.result;
            errno = r.errorno;
        }
        return true; // Continue
    };
    FixedTaskQueue tasks(*worker, result_handler);
    for (auto& name : m_servers) {
        tasks.launch([&name]() {
            debug_info("Ping server (" << name << ")");
            int socket;
            int ret = -1;
            int buffer = socket::xpn_server::PING_CODE;
            // TODO: rethink with different ports
            ret = socket::client_connect(name, DEFAULT_XPN_SERVER_CONTROL_PORT, 5000, socket);
            if (ret < 0) {
                print("[XPN_CONTROLLER] ERROR: socket connection " << name);
                return WorkerResult(ret);
            }

            ret = socket::send(socket, &buffer, sizeof(buffer));
            if (ret < 0) {
                print("[XPN_CONTROLLER] ERROR: socket send ping " << name);
                socket::close(socket);
                return WorkerResult(ret);
            }

            ret = socket::recv(socket, &buffer, sizeof(buffer));
            if (ret < 0) {
                print("[XPN_CONTROLLER] ERROR: socket recv ping " << name);
                socket::close(socket);
                return WorkerResult(ret);
            }
            socket::close(socket);
            return WorkerResult(ret);
        });
    }

    tasks.wait_remaining();

    debug_info("[XPN_CONTROLLER] >> End");
    return res;
}

int xpn_controller::expand(const std::vector<std::string_view>& new_hostlist) {
    debug_info("[XPN_CONTROLLER] >> Start");
    xpn_conf conf;
    auto old_hosts = conf.partitions[0].server_urls;

    int ret = stop_servers(true);
    if (ret < 0) {
        std::cerr << "Error: cannot stop the servers" << std::endl;
        return ret;
    }

    auto xpn_expand_fut = std::async(std::launch::async, [&]() {
        std::string command;
        std::vector<std::string> args;
        if (subprocess::have_command("srun")) {
            command = "srun";
            args.emplace_back("-n");
            args.emplace_back(std::to_string(new_hostlist.size()));
            args.emplace_back("-N");
            args.emplace_back(std::to_string(new_hostlist.size()));
            args.emplace_back("-l");
#ifdef DEBUG
            args.emplace_back("-v");
#endif
            args.emplace_back("--export=ALL");
            std::stringstream servers_list;
            for (uint64_t i = 0; i < new_hostlist.size(); i++) {
                servers_list << new_hostlist[i];
                if (i != (new_hostlist.size() - 1)) {
                    servers_list << ",";
                }
            }
            args.emplace_back("-w");
            args.emplace_back(servers_list.str());
            args.emplace_back("xpn_expand");
            // Path of the servers
            // TODO: current restriction to have all the path the same
            xpn_url url = xpn_parser::parse(old_hosts[0]);
            args.emplace_back(url.path);
            // Last size
            args.emplace_back(std::to_string(old_hosts.size()));
        } else {
            // TODO: with mpiexec
            throw std::runtime_error("TODO: launch with mpiexec the servers");
        }

#ifdef DEBUG
        std::stringstream command_str;
        command_str << command << " ";
        for (auto& arg : args) {
            command_str << arg << " ";
        }
        debug_info("Command to launch: '" << command_str.str() << "'");
#endif

        subprocess::process expand_proc(command, args, false);
        int res = expand_proc.wait_status();
        if (res < 0) {
            print_error("execute xpn_start");
            return res;
        }
        return res;
    });

    ret = update_config(new_hostlist);
    if (ret < 0) {
        std::cerr << "Error: cannot update the config" << std::endl;
        return ret;
    }

    ret = start_servers(true, m_server_cores, false);
    if (ret < 0) {
        std::cerr << "Error: cannot start the servers" << std::endl;
        return ret;
    }

    // Wait the start and the expand
    ret = xpn_expand_fut.get();
    if (ret < 0) {
        print_error("execute xpn_expand");
        return ret;
    }

    debug_info("[XPN_CONTROLLER] >> End");
    return 0;
}

int xpn_controller::shrink(const std::vector<std::string_view>& new_hostlist) {
    debug_info("[XPN_CONTROLLER] >> Start");
    xpn_conf conf;
    auto old_hosts = conf.partitions[0].server_urls;

    std::vector<std::string> old_hostlist;
    get_conf_servers(old_hostlist);

    int ret = stop_servers(true);
    if (ret < 0) {
        std::cerr << "Error: cannot stop the servers" << std::endl;
        return ret;
    }

    auto xpn_expand_fut = std::async(std::launch::async, [&]() {
        std::string command;
        std::vector<std::string> args;
        if (subprocess::have_command("srun")) {
            command = "srun";
            args.emplace_back("-n");
            args.emplace_back(std::to_string(old_hostlist.size()));
            args.emplace_back("-N");
            args.emplace_back(std::to_string(old_hostlist.size()));
            args.emplace_back("-l");
#ifdef DEBUG
            args.emplace_back("-v");
#endif
            args.emplace_back("--export=ALL");
            std::stringstream servers_list;
            for (uint64_t i = 0; i < old_hostlist.size(); i++) {
                servers_list << old_hostlist[i];
                if (i != (old_hostlist.size() - 1)) {
                    servers_list << ",";
                }
            }
            args.emplace_back("-w");
            args.emplace_back(servers_list.str());
            args.emplace_back("xpn_shrink");
            // Path of the servers
            // TODO: current restriction to have all the path the same
            xpn_url url = xpn_parser::parse(old_hosts[0]);
            args.emplace_back(url.path);
            // The new list of servers
            std::stringstream new_servers_list;
            for (uint64_t i = 0; i < new_hostlist.size(); i++) {
                new_servers_list << new_hostlist[i];
                if (i != (new_hostlist.size() - 1)) {
                    new_servers_list << ",";
                }
            }
            args.emplace_back(new_servers_list.str());
        } else {
            // TODO: with mpiexec
            throw std::runtime_error("TODO: launch with mpiexec the servers");
        }

#ifdef DEBUG
        std::stringstream command_str;
        command_str << command << " ";
        for (auto& arg : args) {
            command_str << arg << " ";
        }
        debug_info("Command to launch: '" << command_str.str() << "'");
#endif

        subprocess::process expand_proc(command, args, false);
        int res = expand_proc.wait_status();
        if (res < 0) {
            print_error("execute xpn_start");
            return res;
        }
        return res;
    });

    ret = update_config(new_hostlist);
    if (ret < 0) {
        std::cerr << "Error: cannot update the config" << std::endl;
        return ret;
    }

    ret = start_servers(true, m_server_cores, false);
    if (ret < 0) {
        std::cerr << "Error: cannot start the servers" << std::endl;
        return ret;
    }

    // Wait the start and the expand
    ret = xpn_expand_fut.get();
    if (ret < 0) {
        print_error("execute xpn_expand");
        return ret;
    }

    debug_info("[XPN_CONTROLLER] >> End");
    return 0;
}

int xpn_controller::start_profiler() {
    auto profiler_file = xpn_env::get_instance().xpn_profiler_file;
    if (profiler_file) {
        std::ofstream file(profiler_file);
        if (file.is_open()) {
            file << profiler::get_header();
        } else {
            print_error("Error opening file " << profiler_file);
        }
    }
    return 0;
}

int xpn_controller::end_profiler() {
    auto profiler_file = xpn_env::get_instance().xpn_profiler_file;
    if (profiler_file) {
        std::ofstream file(profiler_file, std::ios::app);
        if (file.is_open()) {
            file << profiler::get_footer();
        } else {
            print_error("Error opening file " << profiler_file);
        }
    }
    return 0;
}
}  // namespace XPN