
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

#include <cstdint>
#include <filesystem>
#include <mutex>
#include <string_view>
#include <unordered_map>

#include "xpn_filesystem/xpn_filesystem.hpp"

namespace XPN {
class xpn_server_db {
   public:
    class xpn_server_last_block {
       public:
        int64_t server_id = -1;
        int64_t last_server_block_offset = -1;
    };
    class xpn_server_block {
       public:
        int64_t block_offset = -1;
        int64_t server_id = -1;
        int64_t server_block_offset = -1;
    };

    constexpr static const char* FILE_EXTENSION_DB_BLOCK = ".xpn_db_block";
    constexpr static const char* FILE_EXTENSION_DB_SERVER = ".xpn_db_server";
    constexpr static int ERROR = -1;
    constexpr static int SUCCESS = 0;
    constexpr static int ERROR_NOT_FOUND = 1;

    int request_block(const std::unique_ptr<xpn_filesystem>& fs, const char* path, int64_t block_offset,
                      int64_t origin_server_id, xpn_server_block& out_block);

   private:
    int get_block(const std::unique_ptr<xpn_filesystem>& fs, const char* path, int64_t block_offset,
                  xpn_server_block& out_block);
    int get_last_block(const std::unique_ptr<xpn_filesystem>& fs, const char* path, int64_t server_id,
                       xpn_server_last_block& out_block);
    int save_block(const std::unique_ptr<xpn_filesystem>& fs, const char* path, const xpn_server_block& block);
    int save_last_block(const std::unique_ptr<xpn_filesystem>& fs, const char* path,
                        const xpn_server_last_block& block);

   private:
    std::mutex m_file_map_mutex;
    class file_map_elem {
       public:
        std::mutex mutex = {};
        int64_t counter = 0;
    };
    class file_map_elem_ref {
       public:
        xpn_server_db& db;
        const char* path;
        file_map_elem* elem = nullptr;

        file_map_elem_ref(xpn_server_db& db, const char* path);
        ~file_map_elem_ref();
    };
    std::unordered_map<std::string, file_map_elem> m_file_map;
};
}  // namespace XPN