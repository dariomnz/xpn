
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

#include <queue>
#include <unordered_map>
#include <unordered_set>

#include "base_cpp/str_unordered_map.hpp"
#include "xpn_file.hpp"

namespace XPN
{
    class xpn_file_table
    {
    public:
        xpn_file_table() = default;
        ~xpn_file_table();
        // Delete copy constructor
        xpn_file_table(const xpn_file_table&) = delete;
        // Delete copy assignment operator
        xpn_file_table& operator=(const xpn_file_table&) = delete;
        // Delete move constructor
        xpn_file_table(xpn_file_table&&) = delete;
        // Delete move assignment operator
        xpn_file_table& operator=(xpn_file_table&&) = delete;
    public:        
        bool has(int fd) {
            std::unique_lock lock(m_mutex);
            return m_files.find(fd) != m_files.end();
        }

        std::shared_ptr<xpn_file> get(int fd) {
            std::unique_lock lock(m_mutex);
            auto it = m_files.find(fd);
            if (it == m_files.end()){
                return nullptr;
            }
            return it->second;
        }

        // int insert(const xpn_file& file);
        int insert(std::shared_ptr<xpn_file> file);

        bool remove(int fd);
        // It must be checked if fd is in the file_table with has(fd)
        int dup(int fd, int new_fd = -1);
        std::string to_string();

        void clean();
        void init_vfhs(const str_unordered_map<std::string, xpn_partition>& partitions);
        void clean_vfhs();

    private:
        std::unordered_map<int, std::shared_ptr<xpn_file>> m_files;
        std::unordered_set<int> m_free_keys;
        int secuencial_key = 1;
        std::recursive_mutex m_mutex = {};
    };
} // namespace XPN