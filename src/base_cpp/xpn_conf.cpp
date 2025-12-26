
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

#include "base_cpp/xpn_conf.hpp"

#include <charconv>
#include <csignal>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include "base_cpp/debug.hpp"
#include "base_cpp/xpn_env.hpp"

namespace XPN {

inline std::string_view trim(std::string_view sv) {
    auto start = sv.find_first_not_of(" \t\n\r");
    if (start == std::string_view::npos) return "";
    auto end = sv.find_last_not_of(" \t\n\r");
    return sv.substr(start, end - start + 1);
}

inline int getSizeFactor(std::string_view name) {
    if (name.empty()) return 0;

    constexpr int KB = 1024;
    constexpr int MB = 1024 * KB;
    constexpr int GB = 1024 * MB;

    int value = 0;

    auto [ptr, ec] = std::from_chars(name.begin(), name.end(), value);
    if (ec != std::errc()) {
        return -1;
    }

    std::string_view suffix = trim(std::string_view(ptr, (name.data() + name.size()) - ptr));

    if (suffix.empty()) return value;
    char unit = suffix[0];
    switch (unit) {
        case 'K':
        case 'k':
            return value * KB;
        case 'M':
        case 'm':
            return value * MB;
        case 'G':
        case 'g':
            return value * GB;
        case 'B':
        case 'b':
            return value;
        default:
            return value;
    }
}

xpn_conf::xpn_conf() {
    const char* cfile_path = xpn_env::get_instance().xpn_conf;
    if (cfile_path == nullptr) {
        std::cerr << "Error: no env variable XPN_CONF" << std::endl;
        std::raise(SIGTERM);
    }

    char file_buffer[4096];
    char line_buffer[1024];

    std::ifstream file;
    file.rdbuf()->pubsetbuf(file_buffer, sizeof(file_buffer));

    file.open(cfile_path);
    if (!file.is_open()) {
        std::cerr << "Error opening file: " << cfile_path << std::endl;
        std::raise(SIGTERM);
    }

    int actual_index = -1;

    while (file.getline(line_buffer, sizeof(line_buffer))) {
        std::string_view sv = trim(line_buffer);
        if (sv.empty()) continue;

        if (sv == XPN_CONF::TAG_PARTITION) {
            partitions.emplace_back();
            actual_index++;
            continue;
        }

        if (actual_index == -1) {
            std::cerr << "Error: " << XPN_CONF::TAG_PARTITION << " not found before data" << std::endl;
            std::raise(SIGTERM);
        }

        size_t sep = sv.find('=');
        if (sep != std::string_view::npos) {
            std::string_view key = trim(sv.substr(0, sep));
            std::string_view value = trim(sv.substr(sep + 1));

            if (key == XPN_CONF::TAG_PARTITION_NAME) {
                partitions[actual_index].partition_name = value;
            } else if (key == XPN_CONF::TAG_CONTROLER_URL) {
                partitions[actual_index].controler_url = value;
            } else if (key == XPN_CONF::TAG_BLOCKSIZE) {
                auto res = getSizeFactor(value);
                if (res < 0) {
                    std::cerr << "Error: Invalid number format for BlockSize: " << value << std::endl;
                    std::raise(SIGTERM);
                }
                partitions[actual_index].bsize = res;
            } else if (key == XPN_CONF::TAG_REPLICATION_LEVEL) {
                int pl = 0;
                auto [ptr, ec] = std::from_chars(value.begin(), value.end(), pl);
                if (ec != std::errc()) {
                    std::cerr << "Error: Invalid replication level: " << value << std::endl;
                    std::raise(SIGTERM);
                }
                partitions[actual_index].replication_level = pl;
            } else if (key == XPN_CONF::TAG_SERVER_URL) {
                partitions[actual_index].server_urls.emplace_back(value);
            } else {
                std::cerr << "Error: unexpected key '" << key << "'" << std::endl;
                std::raise(SIGTERM);
            }
        }
    }

    if (partitions.empty()) {
        std::cerr << "Error: No partitions found in " << cfile_path << std::endl;
        std::raise(SIGTERM);
    }
}
}  // namespace XPN