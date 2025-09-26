
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
// #define DEBUG
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include <cstring>
#include <filesystem>
#include <string>

#include "base_cpp/debug.hpp"
#include "config.h"
#include "dmtcp.h"
#include "xpn.h"

int get_rank() {
    static int rank = -1;
    if (rank != -1) return rank;

    std::string ckpt_dir = dmtcp_get_ckpt_dir();
    debug_info("Checkpoint dir: " << ckpt_dir.c_str());
    std::filesystem::path ckpt_dir_path(ckpt_dir);
    std::string rank_str;
    std::string ckpt_rank_dir = ckpt_dir_path.filename().string();
    if (ckpt_rank_dir.find("ckpt_rank_") != std::string::npos) {
        size_t ckpt_rank_len = std::strlen("ckpt_rank_");
        rank_str = ckpt_rank_dir.substr(ckpt_rank_len, ckpt_rank_dir.size() - ckpt_rank_len);
    } else {
        printf("Error: ckpt_rank_ not found in ckpt dir '%s'\n", ckpt_dir_path.c_str());
        return -1;
    }
    rank = atoi(rank_str.c_str());
    return rank;
}

std::filesystem::path get_ckpt_dir_xpn() {
    std::string ckpt_dir = dmtcp_get_ckpt_dir();
    debug_info("Checkpoint dir: " << ckpt_dir.c_str());
    std::filesystem::path ckpt_dir_path(ckpt_dir);
    std::string ckpt_rank_dir = ckpt_dir_path.filename().string();
    if (ckpt_rank_dir.find("ckpt_rank_") != std::string::npos) {
        ckpt_dir_path = ckpt_dir_path.parent_path();
    }
    debug_info("Checkpoint dir without rank: " << ckpt_dir_path.c_str());
    ckpt_dir_path /= "ckpt_xpn";
    debug_info("Checkpoint dir for xpn: " << ckpt_dir_path.c_str());
    std::error_code ec;
    std::filesystem::create_directory(ckpt_dir_path, ec);
    if (ec) {
        printf("ERROR: create directory %s %s\n", ckpt_dir_path.c_str(), ec.message().c_str());
    }
    return ckpt_dir_path;
}

static std::chrono::time_point start = std::chrono::high_resolution_clock::now();
static void xpn_event_hook(DmtcpEvent_t event, [[maybe_unused]] DmtcpEventData_t *data) {
    int res = 0;

    switch (event) {
        case DMTCP_EVENT_INIT: {
            debug_info("DMTCP_EVENT_INIT");
        } break;

        case DMTCP_EVENT_EXIT: {
            debug_info("DMTCP_EVENT_EXIT");
        } break;

        case DMTCP_EVENT_PRESUSPEND: {
            debug_info("DMTCP_EVENT_PRESUSPEND");
            start = std::chrono::high_resolution_clock::now();
            debug_info("DMTCP_EVENT_PRESUSPEND end = " << res);
        } break;
        case DMTCP_EVENT_PRECHECKPOINT: {
            debug_info("DMTCP_EVENT_PRECHECKPOINT");

            if (get_rank() == 0) {
                debug_info("Execute xpn_flush in rank 0");
                auto ckpt_dir_xpn = get_ckpt_dir_xpn();
                xpn_flush(ckpt_dir_xpn.c_str());
            }

            xpn_clean_connections();

            debug_info("In XPN::flush_barrier");
            dmtcp_global_barrier("XPN::flush_barrier");

            auto now = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double> elapsed_seconds = now - start;
            debug_info("Time taken: " << elapsed_seconds.count());
            start = now;
            debug_info("DMTCP_EVENT_PRECHECKPOINT end = " << res);
        } break;

        case DMTCP_EVENT_RESUME: {
            debug_info("DMTCP_EVENT_RESUME");
            res = xpn_init();
            debug_info("Checkpoint dir: " << dmtcp_get_ckpt_dir());
            auto now = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double> elapsed_seconds = now - start;
            debug_info("Time taken: " << elapsed_seconds.count());
            start = now;
            debug_info("DMTCP_EVENT_RESUME end = " << res);
        } break;

        case DMTCP_EVENT_RESTART: {
            debug_info("DMTCP_EVENT_RESTART");
            res = xpn_init();

            if (get_rank() == 0) {
                debug_info("Execute xpn_preload in rank 0");
                auto ckpt_dir_xpn = get_ckpt_dir_xpn();
                xpn_preload(ckpt_dir_xpn.c_str());
            }

            debug_info("In XPN::preload_barrier");
            dmtcp_global_barrier("XPN::preload_barrier");
            debug_info("DMTCP_EVENT_RESTART end = " << res);
        } break;

        case DMTCP_EVENT_ATFORK_CHILD: {
            debug_info("DMTCP_EVENT_ATFORK_CHILD");
        } break;

        default:
            break;
    }
    fflush(stdin);
}

DmtcpPluginDescriptor_t xpn_plugin = {DMTCP_PLUGIN_API_VERSION, PACKAGE_VERSION, "xpn",         "xpn",
                                      "xpn@gmail.todo",         "XPN plugin",    xpn_event_hook};

DMTCP_DECL_PLUGIN(xpn_plugin);
