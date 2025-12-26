
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

#include <climits>
#include <vector>
#include <string>
#include <sstream>
#include "base_cpp/fixed_string.hpp"
#include "base_cpp/grow_fixed_string.hpp"
#include "base_cpp/grow_fixed_vector.hpp"

namespace XPN
{
    struct xpn_url {
        std::string_view url = "";
        std::string_view protocol = "";
        std::string_view server = "";
        std::string_view port = "";
        std::string_view path = "";
    };

    namespace XPN_CONF
    {
        constexpr const char * TAG_PARTITION = "[partition]";
        constexpr const char * TAG_PARTITION_NAME = "partition_name";
        constexpr const char * TAG_REPLICATION_LEVEL = "replication_level";
        constexpr const char * TAG_BLOCKSIZE = "bsize";
        constexpr const char * TAG_CONTROLER_URL = "controler_url";
        constexpr const char * TAG_SERVER_URL = "server_url";
        constexpr const char * DEFAULT_CONTROLER_URL = "localhost";
        constexpr const int DEFAULT_REPLICATION_LEVEL = 0;
        constexpr const int DEFAULT_BLOCKSIZE = 512 * 1024;
        constexpr const char * DEFAULT_SERVER_TYPE = "mpi";
        constexpr const char * DEFAULT_STORAGE_PATH = "/tmp/expand/data";
        constexpr const char * DEFAULT_PARTITION_NAME= "xpn";
    }

    class xpn_conf
    {
    public:
        struct partition
        {
            GrowFixedString<32> partition_name = XPN_CONF::DEFAULT_PARTITION_NAME;
            int bsize = XPN_CONF::DEFAULT_BLOCKSIZE;
            int replication_level = XPN_CONF::DEFAULT_REPLICATION_LEVEL;
            FixedString<HOST_NAME_MAX> controler_url = XPN_CONF::DEFAULT_CONTROLER_URL;
            std::vector<GrowFixedString<64>> server_urls;

            std::string to_string() const
            {
                std::stringstream out;
                out << XPN_CONF::TAG_PARTITION << std::endl;
                out << XPN_CONF::TAG_PARTITION_NAME << " = " << partition_name << std::endl;
                out << XPN_CONF::TAG_BLOCKSIZE << " = " << bsize << std::endl;
                out << XPN_CONF::TAG_CONTROLER_URL << " = " << controler_url << std::endl;
                out << XPN_CONF::TAG_REPLICATION_LEVEL << " = " << replication_level << std::endl;
                for (auto &srv : server_urls)
                {
                    out << XPN_CONF::TAG_SERVER_URL << " = " << srv << std::endl;
                }
                return out.str();
            }
        };

    public:
        xpn_conf();

        std::string to_string()
        {
            std::stringstream out;
            for (auto &part : partitions)
            {
                out << part.to_string();
            }
            return out.str();
        }
    public:
        // Normaly there are only one partition
        GrowFixedVector<partition, 1> partitions;
    };
}