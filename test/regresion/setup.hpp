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
        XPN_PROFILE_BEGIN_SESSION("Setup servers");
        XPN_PROFILE_FUNCTION();
        std::vector<XPN::subprocess::process> server_processes;

        for (auto&& srv_url : part.server_urls) {
            XPN::xpn_url url = XPN::xpn_parser::parse(srv_url);
            if (url.protocol.empty() || url.server.empty()) {
                std::cerr << "Error: cannot parse server url: '" << srv_url << "'" << std::endl;
                exit(EXIT_FAILURE);
            }
            std::string server_type;
            if (url.protocol == XPN::server_protocols::sck_server) {
                server_type = "sck";
            } else if (url.protocol == XPN::server_protocols::fabric_server) {
                server_type = "fabric";
            } else if (url.protocol == XPN::server_protocols::mpi_server) {
                server_type = "mpi";
            } else {
                std::cerr << "Unsupported protocol to start a server in test " << url.protocol << std::endl;
                exit(EXIT_FAILURE);
            }

            if (url.server != "localhost") {
                std::cerr << "Unsupported server ip to start a server in test " << url.server << std::endl;
                exit(EXIT_FAILURE);
            }
            std::string srv_commmand = "xpn_server -t pool -s " + server_type;
            if (!url.port.empty()) {
                srv_commmand += " --port ";
                srv_commmand += url.port;
            }
            {
                XPN_PROFILE_SCOPE(std::string("start server ") + std::string(url.server));
                XPN::subprocess::process srv_process(srv_commmand, false, false);
                srv_process.set_wait_on_destroy(false);
                server_processes.emplace_back(srv_process);
            }
        }
        XPN_PROFILE_END_SESSION();
        return Defer([processes = std::move(server_processes)]() mutable {
            for (auto&& srv_p : processes) {
                debug_info("Defer kill server is_running" << (srv_p.is_running() ? " true" : " false"));
                srv_p.kill(SIGINT);
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

    static std::string generate_Lorem_Ipsum(size_t targetBytes) {
        if (targetBytes == 0) return "";
        const std::vector<std::string> vocabulary = {
            "lorem",       "ipsum",     "dolor",     "sit",        "amet",        "consectetur",  "adipiscing",
            "elit",        "sed",       "do",        "eiusmod",    "tempor",      "incididunt",   "ut",
            "labore",      "et",        "dolore",    "magna",      "aliqua",      "ut",           "enim",
            "ad",          "minim",     "veniam",    "quis",       "nostrud",     "exercitation", "ullamco",
            "laboris",     "nisi",      "ut",        "aliquip",    "ex",          "ea",           "commodo",
            "consequat",   "duis",      "aute",      "irure",      "dolor",       "in",           "reprehenderit",
            "in",          "voluptate", "velit",     "esse",       "cillum",      "dolore",       "eu",
            "fugiat",      "nulla",     "pariatur",  "excepteur",  "sint",        "occaecat",     "cupidatat",
            "non",         "proident",  "sunt",      "in",         "culpa",       "qui",          "officia",
            "deserunt",    "mollit",    "anim",      "id",         "est",         "laborum",      "at",
            "vero",        "eos",       "et",        "accusamus",  "et",          "iusto",        "odio",
            "dignissimos", "ducimus",   "qui",       "blanditiis", "praesentium", "voluptatum",   "deleniti",
            "atque",       "corrupti",  "quos",      "dolores",    "et",          "quas",         "molestias",
            "excepturi",   "sint",      "occaecati", "cupiditate", "non",         "provident",    "similique",
            "sunt",        "in",        "culpa",     "qui",        "officia",     "deserunt",     "mollitia",
            "animi",       "id",        "est",       "laborum",    "et",          "dolorum",      "fuga",
            "harum",       "quidem",    "rerum",     "facilis",    "est",         "et",           "expedita",
            "distinctio",  "nam",       "libero",    "tempore",    "cum",         "soluta",       "nobis",
            "est",         "eligendi",  "optio",     "cumque",     "nihil",       "impedit",      "quo",
            "minus",       "id",        "quod",      "maxime",     "placeat",     "facere",       "possimus",
            "omnis",       "voluptas",  "assumenda", "est",        "omnis",       "dolor",        "repellendus"};

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, vocabulary.size() - 1);

        std::string result;
        result.reserve(targetBytes);

        while (result.size() < targetBytes) {
            std::string word = vocabulary[dis(gen)];

            if (!result.empty() && result.size() < targetBytes) {
                result += " ";
            }

            size_t remaining = targetBytes - result.size();

            if (word.size() <= remaining) {
                result += word;
            } else {
                result += word.substr(0, remaining);
            }
        }

        if (!result.empty()) {
            result[0] = std::toupper(result[0]);
            result[result.size() - 1] = '.';
        }

        return result;
    }
};

template <typename unit = std::chrono::milliseconds>
class LogTimer {
   public:
    explicit LogTimer(std::string_view name)
        : m_name(name), m_startTimepoint(std::chrono::high_resolution_clock::now()) {}

    ~LogTimer() { stop(); }

    void stop() {
        if (m_stopped) return;

        auto now = std::chrono::high_resolution_clock::now();
        using double_duration = std::chrono::duration<double, typename unit::period>;
        auto elapsed = std::chrono::duration_cast<double_duration>(now - m_startTimepoint).count();

        std::string_view unit_name = get_unit_name();

        std::cout << "[Timer] " << m_name << ": " << std::fixed << std::setprecision(3) << elapsed << " " << unit_name
                  << "\n";

        m_stopped = true;
    }

   private:
    constexpr std::string_view get_unit_name() const {
        if constexpr (std::is_same_v<unit, std::chrono::nanoseconds>)
            return "ns";
        else if constexpr (std::is_same_v<unit, std::chrono::microseconds>)
            return "us";
        else if constexpr (std::is_same_v<unit, std::chrono::milliseconds>)
            return "ms";
        else if constexpr (std::is_same_v<unit, std::chrono::seconds>)
            return "s";
        else
            return " units";
    }

    std::string m_name;
    std::chrono::time_point<std::chrono::high_resolution_clock> m_startTimepoint;
    bool m_stopped = false;
};

class XPN_scope {
   public:
    XPN_scope() {
        LogTimer xpn_init_timer("xpn_init");
        if (xpn_init() < 0) {
            std::cerr << "Error: xpn_init" << std::endl;
            exit(EXIT_FAILURE);
        }
    }
    ~XPN_scope() {
        LogTimer xpn_destroy_timer("xpn_destroy");
        if (xpn_destroy() < 0) {
            std::cerr << "Error: xpn_destroy" << std::endl;
            exit(EXIT_FAILURE);
        }
    }
};