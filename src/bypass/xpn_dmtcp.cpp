
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
#include "xpn_dmtcp.hpp"

#include "base_cpp/allocator.hpp"
// #define DEBUG
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include <cstring>
#include <filesystem>
#include <string>
#include <thread>

#include "base_cpp/debug.hpp"
#include "config.h"
#include "dmtcp.h"
#include "xpn.h"

static std::thread thread_check_ckpt = {};
static std::mutex thread_check_ckpt_running_mutex = {};
static std::condition_variable thread_check_ckpt_running_cv = {};
static bool thread_check_ckpt_running = false;
void thread_check_ckpt_xpn() {
    std::unique_lock lock(thread_check_ckpt_running_mutex);
    printf("INIT thread_check_ckpt_xpn\n");
    while (thread_check_ckpt_running) {
        thread_check_ckpt_running_cv.wait_for(lock, std::chrono::seconds(1));
        if (!thread_check_ckpt_running) break;
        printf("thread_check_ckpt_xpn\n");
    }
    printf("END thread_check_ckpt_xpn\n");
}

void start_thread_check_ckpt_xpn() {
    std::unique_lock lock(thread_check_ckpt_running_mutex);
    if (!thread_check_ckpt_running) {
        printf("start_thread_check_ckpt_xpn\n");
        thread_check_ckpt_running = true;
        thread_check_ckpt = std::thread(thread_check_ckpt_xpn);
    }
}

void stop_thread_check_ckpt_xpn() {
    printf("stop_thread_check_ckpt_xpn\n");
    {
        std::unique_lock lock(thread_check_ckpt_running_mutex);
        thread_check_ckpt_running = false;
        thread_check_ckpt_running_cv.notify_all();
    }

    thread_check_ckpt.join();
}

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

// std::filesystem::path get_ckpt_dir_xpn() {
//     std::string ckpt_dir = dmtcp_get_ckpt_dir();
//     debug_info("Checkpoint dir: " << ckpt_dir.c_str());
//     std::filesystem::path ckpt_dir_path(ckpt_dir);
//     std::string ckpt_rank_dir = ckpt_dir_path.filename().string();
//     if (ckpt_rank_dir.find("ckpt_rank_") != std::string::npos) {
//         ckpt_dir_path = ckpt_dir_path.parent_path();
//     }
//     debug_info("Checkpoint dir without rank: " << ckpt_dir_path.c_str());
//     ckpt_dir_path /= "ckpt_xpn";
//     debug_info("Checkpoint dir for xpn: " << ckpt_dir_path.c_str());
//     std::error_code ec;
//     std::filesystem::create_directory(ckpt_dir_path, ec);
//     if (ec) {
//         printf("ERROR: create directory %s %s\n", ckpt_dir_path.c_str(), ec.message().c_str());
//     }
//     return ckpt_dir_path;
// }

static void printEvent(DmtcpEvent_t event, [[maybe_unused]] DmtcpEventData_t *data) {
    switch (event) {
        case DMTCP_EVENT_INIT:
            debug_info("DMTCP_EVENT_INIT");
            break;
        case DMTCP_EVENT_EXIT:
            debug_info("DMTCP_EVENT_EXIT");
            break;
        case DMTCP_EVENT_PRE_EXEC:
            debug_info("DMTCP_EVENT_PRE_EXEC");
            break;
        case DMTCP_EVENT_POST_EXEC:
            debug_info("DMTCP_EVENT_POST_EXEC");
            break;
        case DMTCP_EVENT_ATFORK_PREPARE:
            debug_info("DMTCP_EVENT_ATFORK_PREPARE");
            break;
        case DMTCP_EVENT_ATFORK_PARENT:
            debug_info("DMTCP_EVENT_ATFORK_PARENT");
            break;
        case DMTCP_EVENT_ATFORK_CHILD:
            debug_info("DMTCP_EVENT_ATFORK_CHILD");
            break;
        case DMTCP_EVENT_ATFORK_FAILED:
            debug_info("DMTCP_EVENT_ATFORK_FAILED");
            break;
        case DMTCP_EVENT_VFORK_PREPARE:
            debug_info("DMTCP_EVENT_VFORK_PREPARE");
            break;
        case DMTCP_EVENT_VFORK_PARENT:
            debug_info("DMTCP_EVENT_VFORK_PARENT");
            break;
        case DMTCP_EVENT_VFORK_CHILD:
            debug_info("DMTCP_EVENT_VFORK_CHILD");
            break;
        case DMTCP_EVENT_VFORK_FAILED:
            debug_info("DMTCP_EVENT_VFORK_FAILED");
            break;
        case DMTCP_EVENT_PTHREAD_START:
            debug_info("DMTCP_EVENT_PTHREAD_START");
            break;
        case DMTCP_EVENT_PTHREAD_EXIT:
            debug_info("DMTCP_EVENT_PTHREAD_EXIT");
            break;
        case DMTCP_EVENT_PTHREAD_RETURN:
            debug_info("DMTCP_EVENT_PTHREAD_RETURN");
            break;
        case DMTCP_EVENT_PRESUSPEND:
            debug_info("DMTCP_EVENT_PRESUSPEND");
            break;
        case DMTCP_EVENT_PRECHECKPOINT:
            debug_info("DMTCP_EVENT_PRECHECKPOINT");
            break;
        case DMTCP_EVENT_POSTCHECKPOINT:
            debug_info("DMTCP_EVENT_POSTCHECKPOINT");
            break;
        case DMTCP_EVENT_RESUME:
            debug_info("DMTCP_EVENT_RESUME");
            break;
        case DMTCP_EVENT_RESTART:
            debug_info("DMTCP_EVENT_RESTART");
            break;
        case DMTCP_EVENT_RUNNING:
            debug_info("DMTCP_EVENT_RUNNING");
            break;
        case DMTCP_EVENT_THREAD_RESUME:
            debug_info("DMTCP_EVENT_THREAD_RESUME");
            break;
        case DMTCP_EVENT_OPEN_FD:
            debug_info("DMTCP_EVENT_OPEN_FD");
            break;
        case DMTCP_EVENT_REOPEN_FD:
            debug_info("DMTCP_EVENT_REOPEN_FD");
            break;
        case DMTCP_EVENT_CLOSE_FD:
            debug_info("DMTCP_EVENT_CLOSE_FD");
            break;
        case DMTCP_EVENT_DUP_FD:
            debug_info("DMTCP_EVENT_DUP_FD");
            break;
        case DMTCP_EVENT_VIRTUAL_TO_REAL_PATH:
            debug_info("DMTCP_EVENT_VIRTUAL_TO_REAL_PATH '" << data->virtualToRealPath.path << "'");
            break;
        case DMTCP_EVENT_REAL_TO_VIRTUAL_PATH:
            debug_info("DMTCP_EVENT_REAL_TO_VIRTUAL_PATH '" << data->realToVirtualPath.path << "'");
            break;
        case nDmtcpEvents:
            debug_info("nDmtcpEvents");
            break;
    }
}

static void xpn_event_hook(DmtcpEvent_t event, [[maybe_unused]] DmtcpEventData_t *data) {
    [[maybe_unused]] int res = 0;
    static std::chrono::time_point start = std::chrono::high_resolution_clock::now();
    // static bool wasDisconnected = false;
    printEvent(event, data);
    switch (event) {
        case DMTCP_EVENT_INIT: {
            debug_info("DMTCP_EVENT_INIT");
        } break;

        case DMTCP_EVENT_EXIT: {
            // stop_thread_check_ckpt_xpn();
            debug_info("DMTCP_EVENT_EXIT");
        } break;

        case DMTCP_EVENT_RUNNING: {
            // start_thread_check_ckpt_xpn();
            debug_info("DMTCP_EVENT_RUNNING");
            xpn_dmtcp::update_restarts();
        } break;

        case DMTCP_EVENT_PRESUSPEND: {
            debug_info("DMTCP_EVENT_PRESUSPEND");
            start = std::chrono::high_resolution_clock::now();
            debug_info("DMTCP_EVENT_PRESUSPEND end = " << res);
        } break;
        case DMTCP_EVENT_PRECHECKPOINT: {
            debug_info("DMTCP_EVENT_PRECHECKPOINT");

            // if (xpn_initialized()) {
            //     xpn_clean_connections();
            //     wasDisconnected = true;
            // }

            debug_info("In XPN::pre_checkpoint");
            dmtcp_global_barrier("XPN::pre_checkpoint");

            auto &instance = XPN::ArenaAllocatorStorage::instance();
            instance.m_inCkpt = true;
            constexpr int ARENA_BUFFER_SIZE = 2 * 1024 * 1024;
            static uint8_t arena_buffer[ARENA_BUFFER_SIZE];
            // Activate a arena for all the operations in ckpt
            instance.activate_arena(arena_buffer, ARENA_BUFFER_SIZE);

            // auto now = std::chrono::high_resolution_clock::now();
            // std::chrono::duration<double> elapsed_seconds = now - start;
            // debug_info("Time taken: " << elapsed_seconds.count());
            // start = now;
            debug_info("DMTCP_EVENT_PRECHECKPOINT end = " << res);
        } break;
        case DMTCP_EVENT_POSTCHECKPOINT: {
            debug_info("DMTCP_EVENT_POSTCHECKPOINT");

            auto &instance = XPN::ArenaAllocatorStorage::instance();
            instance.m_inCkpt = false;
            instance.desactivate_arena();

            std::chrono::duration<double> elapsed_seconds = std::chrono::high_resolution_clock::now() - start;
            debug_info("Time taken PRESUSPEND -> PRECHECKPOINT -> POSTCHECKPOINT: "
                       << std::fixed << std::setprecision(6) << elapsed_seconds.count() << " seconds");
            start = std::chrono::high_resolution_clock::now();
            // if (wasDisconnected) {
            res = xpn_init();
            if (get_rank() == 0) {
                debug_info("Execute xpn_checkpoint in rank 0");
                // auto ckpt_dir_xpn = get_ckpt_dir_xpn();
                // TODO: think how to pass the checkpoint dir
                const char *xpn_checkpoint_dir = std::getenv("XPN_CKPT_DIR");
                if (xpn_checkpoint_dir) {
                    xpn_checkpoint(xpn_checkpoint_dir);
                }
            }
            //     wasDisconnected = false;
            // }

            debug_info("In XPN::post_checkpoint");
            dmtcp_global_barrier("XPN::post_checkpoint");

            xpn_dmtcp::update_restarts();

            elapsed_seconds = std::chrono::high_resolution_clock::now() - start;
            debug_info("Time taken xpn_checkpoint: " << std::fixed << std::setprecision(6) << elapsed_seconds.count()
                                                     << " seconds");
            debug_info("DMTCP_EVENT_POSTCHECKPOINT end = " << res);
        } break;

        case DMTCP_EVENT_RESUME: {
            debug_info("DMTCP_EVENT_RESUME");
            debug_info("Checkpoint dir: " << dmtcp_get_ckpt_dir());
            auto now = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double> elapsed_seconds = now - start;
            debug_info("Time taken: " << elapsed_seconds.count());
            start = now;
            debug_info("DMTCP_EVENT_RESUME end = " << res);
        } break;

        case DMTCP_EVENT_RESTART: {
            debug_info("DMTCP_EVENT_RESTART");
            // In case the saved memory have the arena activated and the in_ckpt activated
            auto &instance = XPN::ArenaAllocatorStorage::instance();
            instance.desactivate_arena();
            instance.m_inCkpt = false;

            xpn_dmtcp::update_restarts();

            xpn_clean_connections();
            // if (wasDisconnected) {
            res = xpn_init();
            // if (get_rank() == 0) {
            //     debug_info("Execute xpn_preload in rank 0");
            // auto ckpt_dir_xpn = get_ckpt_dir_xpn();
            // xpn_preload(ckpt_dir_xpn.c_str());
            // }
            // }

            debug_info("In XPN::preload_barrier");
            dmtcp_global_barrier("XPN::preload_barrier");
            debug_info("DMTCP_EVENT_RESTART end = " << res);
        } break;

        default:
            break;
    }
    fflush(stdout);
}

DmtcpPluginDescriptor_t xpn_plugin = {DMTCP_PLUGIN_API_VERSION, PACKAGE_VERSION, "xpn",         "xpn",
                                      "xpn@gmail.todo",         "XPN plugin",    xpn_event_hook};

DMTCP_DECL_PLUGIN(xpn_plugin);
