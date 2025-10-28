
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

#include <csignal>
#include <cstdio>
#include <iostream>

#include "base_cpp/debug.hpp"
#include "base_cpp/profiler.hpp"
#include "base_cpp/xpn_conf.hpp"
#include "base_cpp/xpn_env.hpp"
#include "xpn/xpn_api.hpp"

namespace XPN {

int xpn_api::initialized() {
    std::unique_lock<std::mutex> lock(m_init_mutex);
    if (m_initialized) {
        return 1;
    }
    return 0;
}

int xpn_api::init() {
    int res = 0;
    XPN_PROFILE_BEGIN_SESSION("xpn_client");
    XPN_DEBUG_BEGIN;

    std::unique_lock<std::mutex> lock(m_init_mutex);
    if (m_initialized) {
        XPN_DEBUG("Already initialized");
        XPN_DEBUG_END;
        return res;
    }
    m_initialized = true;

    std::setbuf(stdout, NULL);
    std::setbuf(stderr, NULL);

    xpn_conf conf;
    xpn_env::get_instance().read_env();

    for (const auto &part : conf.partitions) {
        // Emplace without creation of temp xpn_partition
        auto [key, inserted] =
            m_partitions.emplace(std::piecewise_construct, std::forward_as_tuple(part.partition_name),
                                 std::forward_as_tuple(part.partition_name, part.replication_level, part.bsize));
        if (!inserted) {
            std::cerr << "Error: cannot create xpn_partition" << std::endl;
            std::raise(SIGTERM);
        }
        auto &xpn_part = m_partitions.at(part.partition_name);
        int server_with_error = 0;
        for (const auto &srv_url : part.server_urls) {
            res = xpn_part.init_server(srv_url);
        }

        for (const auto &srv : xpn_part.m_data_serv) {
            if (srv->m_error < 0) {
                server_with_error++;
            }
        }
        if (server_with_error > xpn_part.m_replication_level) {
            std::cerr << "Error: More servers with errors '" << server_with_error << "' than replication level permit '"
                      << xpn_part.m_replication_level << "'" << std::endl;
            std::raise(SIGTERM);
        }
    }

    m_file_table.init_vfhs(m_partitions);

    m_worker = workers::Create(static_cast<workers_mode>(xpn_env::get_instance().xpn_thread));

    XPN_DEBUG_END;
    return res;
}

int xpn_api::destroy() {
    int res = 0;
    XPN_DEBUG_BEGIN;
    std::unique_lock<std::mutex> lock(m_init_mutex);
    if (!m_initialized) {
        XPN_DEBUG("Not initialized");
        XPN_DEBUG_END;
        return res;
    }
    m_initialized = false;

    for (auto &[key, part] : m_partitions) {
        for (auto &serv : part.m_data_serv) {
            serv->destroy_comm();
        }
    }
    m_partitions.clear();
    XPN_DEBUG_END;
    XPN_PROFILE_END_SESSION();
    return res;
}

int xpn_api::print_partitions() {
    printf("Partitions size %d\n", static_cast<int32_t>(m_partitions.size()));
    for (auto &[key, part] : m_partitions) {
        printf("Partition %s:\n", part.m_name.c_str());
        for (auto &serv : part.m_data_serv) {
            printf("  %s\n", serv->m_server.c_str());
        }
    }
    return 0;
}

int xpn_api::clean_connections() {
    int res = 0;
    // Clean file table virtual file handlers
    m_file_table.clean_vfhs();

    res = destroy();

    return res;
}

int xpn_api::mark_error_server(int index) {
    int res = -1;
    XPN_DEBUG_BEGIN;
    for (auto &[key, part] : m_partitions) {
        if (index < static_cast<int>(part.m_data_serv.size())) {
            part.m_data_serv[index]->m_error = -1;
            res = 0;
        }
    }
    XPN_DEBUG_END;
    return res;
}

int xpn_api::get_block_locality(char *path, int64_t offset, int *url_c, char **url_v[]) {
    XPN_DEBUG_BEGIN_CUSTOM(path << ", " << offset);
    int res = 0;
    int64_t local_offset;
    int serv;

    std::string file_path;
    auto part_name = check_remove_part_from_path(path, file_path);
    if (part_name.empty()) {
        errno = ENOENT;
        XPN_DEBUG_END_CUSTOM(path << ", " << offset);
        return -1;
    }

    xpn_file file(file_path, m_partitions.at(part_name));

    res = read_metadata(file.m_mdata);

    if (res < 0 || !file.m_mdata.m_data.is_valid()) {
        XPN_DEBUG_END_CUSTOM(path << ", " << offset);
        return -1;
    }

    (*url_v) = (char **)malloc(((file.m_part.m_replication_level + 1) + 1) * sizeof(char *));
    if ((*url_v) == NULL) {
        XPN_DEBUG_END_CUSTOM(path << ", " << offset);
        return -1;
    }

    for (int i = 0; i < file.m_part.m_replication_level + 1; i++) {
        (*url_v)[i] = (char *)malloc(PATH_MAX * sizeof(char));
        if ((*url_v)[i] == NULL) {
            XPN_DEBUG_END_CUSTOM(path << ", " << offset);
            return -1;
        }
        memset((*url_v)[i], 0, PATH_MAX);
    }

    (*url_v)[file.m_part.m_replication_level + 1] = NULL;

    for (int i = 0; i < file.m_part.m_replication_level + 1; i++) {
        file.map_offset_mdata(offset, i, local_offset, serv);
        auto &serv_url = file.m_part.m_data_serv[serv]->m_server;
        serv_url.copy((*url_v)[i], serv_url.size());
    }

    (*url_c) = file.m_part.m_replication_level + 1;

    XPN_DEBUG_END_CUSTOM(path << ", " << offset);
    return res;
}

int xpn_api::free_block_locality(int *url_c, char **url_v[]) {
    XPN_DEBUG_BEGIN;
    int res = 0;
    for (int i = 0; i < (*url_c); i++) {
        free((*url_v)[i]);
    }

    free((*url_v));

    (*url_v) = NULL;
    (*url_c) = 0;
    XPN_DEBUG_END;
    return res;
}

int xpn_api::flush_preload(const char *path, bool isFlush) {
    XPN_DEBUG_BEGIN_CUSTOM(path);
    int res = 0;
    // TODO: support more than one partition
    auto &part = m_partitions.begin()->second;
    int serv_done = 0;
    for (auto &&serv : part.m_data_serv) {
        if (isFlush) {
            res = serv->nfi_flush(path);
        } else {
            res = serv->nfi_preload(path);
        }
        if (res < 0) {
            break;
        }
        serv_done++;
    }

    int error = 0;
    for (int i = 0; i < serv_done; i++) {
        res = part.m_data_serv[i]->nfi_response();
        if (res < 0) {
            error = res;
            continue;
        }
    }

    if (error) {
        res = error;
    }

    XPN_DEBUG_END_CUSTOM(path);
    return res;
}

int xpn_api::checkpoint(const char *path) {
    XPN_DEBUG_BEGIN_CUSTOM(path);
    int res = 0;
    // TODO: support more than one partition
    auto &part = m_partitions.begin()->second;
    int serv_done = 0;
    for (auto &&serv : part.m_data_serv) {
        res = serv->nfi_checkpoint(path);
        if (res < 0) {
            break;
        }
        serv_done++;
    }

    int error = 0;
    for (int i = 0; i < serv_done; i++) {
        res = part.m_data_serv[i]->nfi_response();
        if (res < 0) {
            error = res;
            continue;
        }
    }

    if (error) {
        res = error;
    }

    XPN_DEBUG_END_CUSTOM(path);
    return res;
}
}  // namespace XPN
