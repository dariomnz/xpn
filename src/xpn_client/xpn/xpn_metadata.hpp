
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
#include <sstream>
#include <vector>
#include <array>

#include "base_cpp/xpn_path.hpp"
#include "base_cpp/debug.hpp"

namespace XPN
{
    
    // Fordward declaration
    class xpn_file;

    class xpn_metadata
    {
    public:
        static constexpr const int HEADER_SIZE = 8192;
        static constexpr const char * MAGIC_NUMBER = "XPN";
        static constexpr const int VERSION = 1;
        static constexpr const int MAX_RECONSTURCTIONS = 40;
        static constexpr const int DISTRIBUTION_ROUND_ROBIN = 1;
    public:
        struct data{
            void fill(const xpn_metadata& mdata);

            char     magic_number[4] = {'0', '0', '0', '0'};    // Magic number to identify if is correct the metadata
            int32_t  version = 0;                               // Version number
            int32_t  type = 0;                                  // Type of file: file or directory
            int32_t  replication_level = 0;                     // Replication level of files: 0, 1, 2, ...
            int32_t  first_node = 0;                            // Server which has the first block
            int32_t  distribution_policy = 0;                   // Distribution policy of blocks, default: round-robin
            uint64_t block_size = 0;                            // Size of block used
            uint64_t file_size = 0;                             // Size of the file
            int32_t  data_nserv[MAX_RECONSTURCTIONS] = {0};     // Array of number of servers to reconstruct
            int32_t  offsets[MAX_RECONSTURCTIONS] = {0};        // Array indicating the block where new server configuration starts
            
            bool is_valid() { 
                return magic_number[0] == MAGIC_NUMBER[0] && 
                       magic_number[1] == MAGIC_NUMBER[1] && 
                       magic_number[2] == MAGIC_NUMBER[2];
            } 
            friend std::ostream& operator<<(std::ostream& os, const data& d) {
                os <<"magic_number: " << d.magic_number[0] << d.magic_number[1] << d.magic_number[2] << std::endl;
                os <<"version: " << d.version << std::endl;
                os <<"type: " << d.type << std::endl;
                os <<"block_size: " << d.block_size << std::endl;
                os <<"file_size: " << d.file_size << std::endl;
                os <<"replication_level: " << d.replication_level << std::endl;
                os <<"first_node: " << d.first_node << std::endl;
                os <<"distribution_policy: " << d.distribution_policy << std::endl;
                os <<"data_nserv: ";
                for(int i = 0; i < xpn_metadata::MAX_RECONSTURCTIONS; i++) {
                    os << d.data_nserv[i] << " ";
                }
                os << std::endl;

                os <<"offsets: ";
                for(int i = 0; i < xpn_metadata::MAX_RECONSTURCTIONS; i++) {
                    os << d.offsets[i] << " ";
                }
                os << std::endl;
                return os;
            }
        };
    public:
        xpn_metadata(xpn_file& file) : m_file(file) {}
        xpn_metadata(xpn_file& file, const xpn_metadata::data &data) : m_file(file), m_data(data) {}
        // Delete default constructors
        xpn_metadata() = delete;
        // Delete copy constructor
        xpn_metadata(const xpn_metadata&) = delete;
        // Delete copy assignment operator
        xpn_metadata& operator=(const xpn_metadata&) = delete;
        // Delete move constructor
        xpn_metadata(xpn_metadata&&) = delete;
        // Delete move assignment operator
        xpn_metadata& operator=(xpn_metadata&&) = delete;
    public:
        xpn_file &m_file;
        data m_data;

    public:
    private:
        int calculate_master(bool is_file) const;
    public:
        int master_file() const {return calculate_master(true);}
        int master_dir() const {return calculate_master(false);}
        friend std::ostream& operator<<(std::ostream& os, const xpn_metadata& mdata); 
        std::string to_string_blocks(int blocks);
    };
} // namespace XPN