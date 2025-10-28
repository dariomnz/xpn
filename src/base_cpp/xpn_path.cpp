
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

#include "xpn_path.hpp"

#include <filesystem>
namespace XPN {

std::string_view xpn_path::get_first_dir(std::string_view path) {
    // Skip leading slashes
    size_t start = 0;
    while (start < path.size() && path[start] == '/') ++start;

    // Find the next slash
    size_t end = start;
    while (end < path.size() && path[end] != '/') ++end;

    // Return the first directory as a string_view
    if (start < end) return path.substr(start, end - start);

    return std::string_view{};
}

std::string_view xpn_path::remove_first_dir(std::string_view path) {
    // Skip leading slashes
    size_t start = 0;
    while (start < path.size() && path[start] == '/') ++start;

    // Find the end of the first directory
    size_t end = start;
    while (end < path.size() && path[end] != '/') ++end;

    // Skip any slashes after the first directory
    while (end < path.size() && path[end] == '/') ++end;

    // Return the rest of the path
    if (end < path.size()) return path.substr(end);

    return std::string_view{};
}

int xpn_path::hash(std::string_view path, int max_num, bool is_file) {
    size_t last_slash = path.find_last_of("/\\");
    std::string_view name;

    if (is_file) {
        if (last_slash != std::string_view::npos)
            name = path.substr(last_slash + 1);
        else
            name = path;
    } else {
        if (last_slash != std::string_view::npos) {
            std::string_view parent = path.substr(0, last_slash);

            size_t prev_slash = parent.find_last_of("/\\");
            if (prev_slash != std::string_view::npos)
                name = parent.substr(prev_slash + 1);
            else
                name = parent;
        } else {
            name = path;
        }
    }

    int num = 0;
    for (char ch : name) {
        num += static_cast<int>(ch);
    }

    return max_num > 0 ? num % max_num : num;
}
}  // namespace XPN