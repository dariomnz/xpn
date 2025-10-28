#pragma once

#include <stdlib.h>
#include <xpn.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "base_cpp/debug.hpp"
#include "base_cpp/subprocess.hpp"
#include "base_cpp/xpn_conf.hpp"
#include "base_cpp/xpn_parser.hpp"
#include "xpn_client/nfi/nfi_xpn_server_comm.hpp"

// Forward declaration
class Defer;
static std::unordered_set<Defer*> s_defers_atexit = {};
class Defer {
   public:
    Defer(std::function<void()> func) : m_cleanup(func) {
        static std::once_flag once_flag;
        std::call_once(once_flag, []() {
            debug_info("Register atexit execution of defers");
            std::atexit([]() {
                debug_info("Execute " << s_defers_atexit.size() << " at exit");
                for (auto&& d : s_defers_atexit) {
                    if (d->m_should_run_cleanup) {
                        d->m_cleanup();
                        d->m_should_run_cleanup = false;
                    }
                }
                s_defers_atexit.clear();
            });
        });

        debug_info("Add defer to exit");
        s_defers_atexit.emplace(this);
    }

    ~Defer() noexcept {
        if (m_should_run_cleanup) {
            m_cleanup();
            m_should_run_cleanup = false;
        }
        debug_info("Remove defer to exit");
        s_defers_atexit.erase(this);
    }

   private:
    std::function<void()> m_cleanup;
    bool m_should_run_cleanup = true;
};

extern char** environ;
class setup {
   public:
    static void env(const std::unordered_map<std::string, std::string>& envs) {
        for (auto&& [k, v] : envs) {
            if (::setenv(k.c_str(), v.c_str(), 1) != 0) {
                std::cerr << "Error setting enviroment '" << k << "' to '" << v << "'" << std::endl;
                std::exit(EXIT_FAILURE);
            }
        }
    }

    static void print_env() {
        char** env_ptr = environ;

        while (*env_ptr != nullptr) {
            std::cout << *env_ptr << std::endl;
            env_ptr++;
        }
    }

    [[nodiscard]] static Defer create_empty_dir(const std::string& path) {
        if (std::filesystem::exists(path)) {
            std::filesystem::remove_all(path);
        }

        if (!std::filesystem::create_directories(path)) {
            std::cerr << "Error creating directory '" << path << "'" << std::endl;
            std::exit(EXIT_FAILURE);
        }

        return Defer([path]() {
            debug_info("Defer remove path '" << path << "'");
            if (std::filesystem::exists(path)) {
                std::filesystem::remove_all(path);
            }
        });
    }

    [[nodiscard]] static Defer create_xpn_conf(const std::string path, const XPN::xpn_conf::partition& part) {
        std::ofstream file(path);

        if (file.is_open()) {
            file << part.to_string();
            file.close();
        } else {
            std::cerr << "Error: Could not open file " << path << " for writing" << std::endl;
            std::exit(EXIT_FAILURE);
        }

        if (::setenv("XPN_CONF", path.c_str(), 1) != 0) {
            std::cerr << "Error setting enviroment XPN_CONF to '" << path << "'" << std::endl;
            std::exit(EXIT_FAILURE);
        }

        return Defer([path]() {
            debug_info("Defer remove path '" << path << "'");
            if (std::filesystem::exists(path)) {
                std::filesystem::remove(path);
            }
        });
    }

    [[maybe_unused]] static Defer start_srvs(const XPN::xpn_conf::partition& part) {
        std::vector<XPN::subprocess::process> server_processes;

        for (auto&& srv_url : part.server_urls) {
            std::string protocol, server, port;
            std::tie(protocol, server, port, std::ignore) = XPN::xpn_parser::parse(srv_url);
            if (protocol.empty() || server.empty()) {
                std::cerr << "Error: cannot parse server url: '" << srv_url << "'" << std::endl;
                exit(EXIT_FAILURE);
            }
            std::string server_type;
            if (protocol == XPN::server_protocols::sck_server) {
                server_type = "sck";
            } else if (protocol == XPN::server_protocols::fabric_server) {
                server_type = "fabric";
            } else if (protocol == XPN::server_protocols::mpi_server) {
                server_type = "mpi";
            } else {
                std::cerr << "Unsupported protocol to start a server in test " << protocol << std::endl;
                exit(EXIT_FAILURE);
            }

            if (server != "localhost") {
                std::cerr << "Unsupported server ip to start a server in test " << server << std::endl;
                exit(EXIT_FAILURE);
            }
            std::string srv_commmand = "xpn_server -t pool -s " + server_type;
            if (!port.empty()) {
                srv_commmand += " --port " + port;
            }
            XPN::subprocess::process srv_process(srv_commmand, false);
            srv_process.set_wait_on_destroy(false);
            server_processes.emplace_back(srv_process);
        }

        return Defer([processes = std::move(server_processes)]() mutable {
            for (auto&& srv_p : processes) {
                srv_p.kill(SIGKILL);
                srv_p.wait_status();
            }
        });
    }

    static std::string generate_random_string(size_t length) {
        const std::string characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        std::string result;
        result.reserve(length);

        std::random_device rd;
        std::mt19937 generator(rd());
        std::uniform_int_distribution<size_t> distribution(0, characters.length() - 1);

        for (size_t i = 0; i < length; ++i) {
            result += characters[distribution(generator)];
        }
        return result;
    }
};

class XPN_scope {
   public:
    XPN_scope() {
        if (xpn_init() < 0) {
            std::cerr << "Error: xpn_init" << std::endl;
            exit(EXIT_FAILURE);
        }
    }
    ~XPN_scope() {
        if (xpn_destroy() < 0) {
            std::cerr << "Error: xpn_destroy" << std::endl;
            exit(EXIT_FAILURE);
        }
    }
};