
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

#include <string>
#include <vector>
#include <dirent.h>

#include <xpn/xpn_partition.hpp>
#include <xpn/xpn_metadata.hpp>
#include "base_cpp/grow_fixed_string.hpp"

namespace XPN
{
    struct xpn_fh
    {
        GrowFixedString<128> path;  // url in the server   
        long telldir = 0;           // telldir of directory in the server when XPN_SESSION_DIR is not set
        int64_t dir = 0;            // pointer to directory in the server when XPN_SESSION_DIR set
        int fd = -1;                // file_descriptor in the server when XPN_SESSION_FILE set

        xpn_fh() { path.reserve(xpn_env::get_instance().xpn_reserve_path_vfh); }
        bool is_initialized() {return !path.empty();}
        void reset() {*this = {};}
    };

    enum class file_type
    {
        null = 0,
        file = 1,
        dir = 2
    };

    class xpn_file
    {
    public:
        xpn_file(std::string_view path, const xpn_partition &part) : m_path(path), m_part(part), m_mdata(*this) 
        {
            m_data_vfh.resize(m_part.m_data_serv.size());
        }
        static std::shared_ptr<xpn_file> change_part(std::shared_ptr<xpn_file>& file, const xpn_partition &new_part) {
            auto new_file = std::make_shared<xpn_file>(file->m_path, new_part);
            new_file->m_type = file->m_type;
            new_file->m_links = file->m_links;
            new_file->m_flags = file->m_flags;
            new_file->m_mode = file->m_mode;
            new_file->m_offset = file->m_offset;
            new_file->m_mdata.m_data = file->m_mdata.m_data;
            return new_file;
        }
        // Delete default constructors
        xpn_file() = delete;
        // Delete copy constructor
        xpn_file(const xpn_file&) = delete;
        // Delete copy assignment operator
        xpn_file& operator=(const xpn_file&) = delete;
        // Delete move constructor
        xpn_file(xpn_file&&) = delete;
        // Delete move assignment operator
        xpn_file& operator=(xpn_file&&) = delete;
    public:
        bool exist_in_serv(int serv);
        void static map_offset(int block_size, int replication_level, int nserv, int64_t offset, int replication, int first_node, int64_t &local_offset, int &serv);
        void static inverted_map_offset(int block_size, int replication_level, int nserv, int serv, int64_t local_offset, int first_node, int64_t &offset, int &replication);
        void map_offset_mdata(int64_t offset, int replication, int64_t &local_offset, int &serv);
        int  initialize_vfh(int index);
        int  initialize_vfh_dir(int index);

    public:
        GrowFixedString<128> m_path;                 // absolute path
        file_type m_type = file_type::null; // indicate FILE or DIR
        int m_links = 0;                    // number of links that this file has
        int m_flags = 0;                    // O_RDONLY, O_WRONLY,....    
        mode_t m_mode = 0;                  // S_IRUSR , S_IWUSR ,....  
        const xpn_partition &m_part;              // partition
        xpn_metadata m_mdata;               // metadata
        int64_t m_offset = 0;                 // offset of the open file
        std::vector<xpn_fh> m_data_vfh;     // virtual FH

    };
} // namespace XPN