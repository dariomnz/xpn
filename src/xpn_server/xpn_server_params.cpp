
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

/* ... Include / Inclusion ........................................... */

#include "xpn_server_params.hpp"

#include "base_cpp/debug.hpp"
#include "base_cpp/ns.hpp"
#include "base_cpp/workers.hpp"
#include "xpn_server/filesystem/xpn_server_filesystem.hpp"

namespace XPN {

/* ... Functions / Funciones ......................................... */

void xpn_server_params::show() {
    debug_info("[Server=" << ns::get_host_name() << "] [XPN_SERVER_PARAMS] [xpn_server_params_show] >> Begin");

    printf(" | * XPN server current configuration:\n");

    printf(" |\tcontrol port: \t%d\n", srv_control_port);
    if (srv_comm_port != DEFAULT_XPN_SERVER_COMM_PORT) {
        printf(" |\tcomm port: \t%d\n", srv_comm_port);
    }
    if (srv_connectionless_port != DEFAULT_XPN_SERVER_CONNECTIONLESS_PORT) {
        printf(" |\tconnectionless port: \t%d\n", srv_connectionless_port);
    }
    // * server_type
    if (srv_type == server_type::MPI) {
        printf(" |\tserver type: \tmpi_server\n");
    } else if (srv_type == server_type::SCK) {
        printf(" |\tserver type: \tsck_server\n");
    } else if (srv_type == server_type::MQTT) {
        printf(" |\tserver type: \tmqtt_server\n");
    } else if (srv_type == server_type::FABRIC) {
        printf(" |\tserver type: \tfabric_server\n");
    } else {
        printf(" |\tserver type: \tError: unknown\n");
    }

    // * threads
    if (thread_mode == workers_mode::sequential) {
        printf(" |\tthread mode: \tWithout threads\n");
    } else if (thread_mode == workers_mode::thread_pool) {
        printf(" |\tthread mode: \tThread Pool Activated\n");
    } else if (thread_mode == workers_mode::thread_on_demand) {
        printf(" |\tthread mode: \tThread on demand\n");
    } else {
        printf(" |\tthread mode: \tError: unknown\n");
    }

    // * shutdown_file
    if (!shutdown_file.empty()) {
        printf(" |\tshutdown_file: \t'%s'\n", shutdown_file.c_str());
    }
    // * await
    if (await_stop == 1) {
        printf(" |\tawait: \ttrue\n");
    }
    if (fs_mode == filesystem_mode::xpn) {
        printf(" |\tproxy mode: \ton\n");
    } else if (fs_mode == filesystem_mode::memory) {
        printf(" |\tmemoty mode: \ton\n");
    }
    if (mqtt_qos != DEFAULT_XPN_SERVER_MQTT_QOS || srv_type == server_type::MQTT) {
        printf(" |\tmqtt qos: \t%d\n", mqtt_qos);
    }

    debug_info("[Server=" << ns::get_host_name() << "] [XPN_SERVER_PARAMS] [xpn_server_params_show] << End");
}

void xpn_server_params::show_usage() {
    debug_info("[Server=" << ns::get_host_name() << "] [XPN_SERVER_PARAMS] [xpn_server_params_show_usage] >> Begin");

    printf("Usage:\n");
    printf("\t-p, --port            <port>        port used by the contol socket (default: 3456)\n");
    printf("\t-s, --server_type     <server_type> mpi, sck, fabric (default: sck)\n");
    printf("\t-t, --thread_mode     <int>         0 (without); 1 (pool); 2 (on_demand) (default: without)\n");
    printf("\t-f, --shutdown_file   <path>        file of servers to be shutdown\n");
    printf("\t-d, --shutdown_hosts  <list>        comma separated list of hosts to be shutdown\n");
    printf("\t-m, --mqtt_qos        <qos>         enable mqtt with that qos\n");
    printf("\t--connectionless_port <port>        port used by the connectionless socket (default: 0)\n");
    printf("\t--comm_port           <port>        port used by the communcation socket (default: 0)\n");
    printf("\t-w, --await                         await for servers to stop\n");
    printf("\t-x, --proxy                         activate proxy mode\n");
    printf("\t-h, --help                          print this usage information\n");

    debug_info("[Server=" << ns::get_host_name() << "] [XPN_SERVER_PARAMS] [xpn_server_params_show_usage] << End");
}

xpn_server_params::xpn_server_params(int _argc, char *_argv[]) {
    debug_info("[Server=" << ns::get_host_name() << "] [XPN_SERVER_PARAMS] [xpn_server_params_get] >> Begin");

    // set default values
    argc = _argc;
    argv = _argv;
    size = 0;
    rank = 0;
    thread_mode = workers_mode::sequential;
    srv_type = server_type::SCK;

    fs_mode = filesystem_mode::disk;
    await_stop = 0;
    mqtt_qos = DEFAULT_XPN_SERVER_MQTT_QOS;
    srv_control_port = DEFAULT_XPN_SERVER_CONTROL_PORT;
    srv_comm_port = DEFAULT_XPN_SERVER_COMM_PORT;
    srv_connectionless_port = DEFAULT_XPN_SERVER_CONNECTIONLESS_PORT;

    // update user requests
    debug_info("[Server=" << ns::get_host_name()
                          << "] [XPN_SERVER_PARAMS] [xpn_server_params_get] Get user configuration");
    int idx = 1;
    while (idx < argc) {
        std::string_view arg = argv[idx];
        if (arg == "-h" || arg == "--help") {
            show_usage();
            exit(EXIT_SUCCESS);
        } else if (arg == "-f" || arg == "--shutdown_file") {
            shutdown_file = ++idx >= argc ? "" : argv[idx];
        } else if (arg == "-d" || arg == "--shutdown_hosts") {
            shutdown_hostlist = ++idx >= argc ? "" : argv[idx];
        } else if (arg == "-w" || arg == "--await") {
            await_stop = 1;
        } else if (arg == "--memory") {
            fs_mode = filesystem_mode::memory;
        } else if (arg == "-x" || arg == "--proxy") {
            fs_mode = filesystem_mode::xpn;
        } else if (arg == "-p" || arg == "--port") {
            srv_control_port = ++idx >= argc ? DEFAULT_XPN_SERVER_CONTROL_PORT : atoi(argv[idx]);
        } else if (arg == "-m" || arg == "--mqtt_qos") {
            mqtt_qos = ++idx >= argc ? DEFAULT_XPN_SERVER_MQTT_QOS : atoi(argv[idx]);
        } else if (arg == "--connectionless_port") {
            srv_connectionless_port = ++idx >= argc ? DEFAULT_XPN_SERVER_CONNECTIONLESS_PORT : atoi(argv[idx]);
        } else if (arg == "--comm_port") {
            srv_comm_port = ++idx >= argc ? DEFAULT_XPN_SERVER_COMM_PORT : atoi(argv[idx]);
        } else if (arg == "-t" || arg == "--thread_mode") {
            if (++idx < argc) {
                if (isdigit(argv[idx][0])) {
                    int thread_mode_aux = atoi(argv[idx]);

                    if (thread_mode_aux >= static_cast<int>(workers_mode::sequential) &&
                        thread_mode_aux <= static_cast<int>(workers_mode::thread_on_demand)) {
                        thread_mode = static_cast<workers_mode>(thread_mode_aux);
                    } else {
                        printf("ERROR: unknown option %s\n", argv[idx]);
                        show_usage();
                    }
                } else {
                    if (strcmp("without", argv[idx]) == 0) {
                        thread_mode = workers_mode::sequential;
                    } else if (strcmp("pool", argv[idx]) == 0) {
                        thread_mode = workers_mode::thread_pool;
                    } else if (strcmp("on_demand", argv[idx]) == 0) {
                        thread_mode = workers_mode::thread_on_demand;
                    } else {
                        printf("ERROR: unknown option %s\n", argv[idx]);
                        show_usage();
                    }
                }
            }
        } else if (arg == "-s" || arg == "--server_type") {
            if (++idx < argc) {
                if (strcmp("mpi", argv[idx]) == 0) {
                    srv_type = server_type::MPI;
                } else if (strcmp("sck", argv[idx]) == 0) {
                    srv_type = server_type::SCK;
                } else if (strcmp("mqtt", argv[idx]) == 0) {
                    srv_type = server_type::MQTT;
                } else if (strcmp("fabric", argv[idx]) == 0) {
                    srv_type = server_type::FABRIC;
                } else {
                    printf("ERROR: unknown option %s\n", argv[idx]);
                    show_usage();
                }
            }
        } else {
            std::cout << "unexpected option '" << arg << "'\n";
            show_usage();
            exit(EXIT_FAILURE);
            break;
        }

        ++idx;
    }

    debug_info("[Server=" << ns::get_host_name() << "] [XPN_SERVER_PARAMS] [xpn_server_params_get] << End");
}

/* ................................................................... */

}  // namespace XPN
