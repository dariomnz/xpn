
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

#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <unordered_map>

namespace XPN {

struct string_hash {
    using is_transparent = void;
    [[nodiscard]] size_t operator()(const char *txt) const { return std::hash<std::string_view>{}(txt); }
    [[nodiscard]] size_t operator()(std::string_view txt) const { return std::hash<std::string_view>{}(txt); }
    [[nodiscard]] size_t operator()(const std::string &txt) const { return std::hash<std::string>{}(txt); }
};

// This str_unordered_map permit the hash of string_view string and const char* in find
template <typename K, typename V>
using str_unordered_map = std::unordered_map<K, V, string_hash, std::equal_to<>>;
}  // namespace XPN
