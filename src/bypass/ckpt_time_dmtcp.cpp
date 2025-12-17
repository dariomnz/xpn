
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

#include <unistd.h>

#include <chrono>
#include <unordered_map>

#include "config.h"
#include "dmtcp.h"

[[maybe_unused]] static void ckptTimePrintEvent(DmtcpEvent_t event, [[maybe_unused]] DmtcpEventData_t *data) {
    static const std::unordered_map<DmtcpEvent_t, const char *> eventNames = {
        {DMTCP_EVENT_INIT, "DMTCP_EVENT_INIT"},
        {DMTCP_EVENT_EXIT, "DMTCP_EVENT_EXIT"},
        {DMTCP_EVENT_PRE_EXEC, "DMTCP_EVENT_PRE_EXEC"},
        {DMTCP_EVENT_POST_EXEC, "DMTCP_EVENT_POST_EXEC"},
        {DMTCP_EVENT_ATFORK_PREPARE, "DMTCP_EVENT_ATFORK_PREPARE"},
        {DMTCP_EVENT_ATFORK_PARENT, "DMTCP_EVENT_ATFORK_PARENT"},
        {DMTCP_EVENT_ATFORK_CHILD, "DMTCP_EVENT_ATFORK_CHILD"},
        {DMTCP_EVENT_ATFORK_FAILED, "DMTCP_EVENT_ATFORK_FAILED"},
        {DMTCP_EVENT_VFORK_PREPARE, "DMTCP_EVENT_VFORK_PREPARE"},
        {DMTCP_EVENT_VFORK_PARENT, "DMTCP_EVENT_VFORK_PARENT"},
        {DMTCP_EVENT_VFORK_CHILD, "DMTCP_EVENT_VFORK_CHILD"},
        {DMTCP_EVENT_VFORK_FAILED, "DMTCP_EVENT_VFORK_FAILED"},
        {DMTCP_EVENT_PTHREAD_START, "DMTCP_EVENT_PTHREAD_START"},
        {DMTCP_EVENT_PTHREAD_EXIT, "DMTCP_EVENT_PTHREAD_EXIT"},
        {DMTCP_EVENT_PTHREAD_RETURN, "DMTCP_EVENT_PTHREAD_RETURN"},
        {DMTCP_EVENT_PRESUSPEND, "DMTCP_EVENT_PRESUSPEND"},
        {DMTCP_EVENT_PRECHECKPOINT, "DMTCP_EVENT_PRECHECKPOINT"},
        {DMTCP_EVENT_POSTCHECKPOINT, "DMTCP_EVENT_POSTCHECKPOINT"},
        {DMTCP_EVENT_RESUME, "DMTCP_EVENT_RESUME"},
        {DMTCP_EVENT_RESTART, "DMTCP_EVENT_RESTART"},
        {DMTCP_EVENT_RUNNING, "DMTCP_EVENT_RUNNING"},
        {DMTCP_EVENT_THREAD_RESUME, "DMTCP_EVENT_THREAD_RESUME"},
        {DMTCP_EVENT_OPEN_FD, "DMTCP_EVENT_OPEN_FD"},
        {DMTCP_EVENT_REOPEN_FD, "DMTCP_EVENT_REOPEN_FD"},
        {DMTCP_EVENT_CLOSE_FD, "DMTCP_EVENT_CLOSE_FD"},
        {DMTCP_EVENT_DUP_FD, "DMTCP_EVENT_DUP_FD"},
        {DMTCP_EVENT_VIRTUAL_TO_REAL_PATH, "DMTCP_EVENT_VIRTUAL_TO_REAL_PATH"},
        {DMTCP_EVENT_REAL_TO_VIRTUAL_PATH, "DMTCP_EVENT_REAL_TO_VIRTUAL_PATH"},
        {nDmtcpEvents, "nDmtcpEvents"}};

    auto it = eventNames.find(event);
    if (it != eventNames.end()) {
        if (event == DMTCP_EVENT_VIRTUAL_TO_REAL_PATH) {
            printf("CKPT TIME PLUGIN: %s '%s'\n", it->second, data->virtualToRealPath.path);
        } else if (event == DMTCP_EVENT_REAL_TO_VIRTUAL_PATH) {
            printf("CKPT TIME PLUGIN: %s '%s'\n", it->second, data->realToVirtualPath.path);
        } else {
            printf("CKPT TIME PLUGIN: %s\n", it->second);
        }
    } else {
        printf("CKPT TIME PLUGIN: Undefined DmtcpEvent_t\n");
    }
    fflush(stdout);
}

static void ckpt_time_event_hook(DmtcpEvent_t event, [[maybe_unused]] DmtcpEventData_t *data) {
    [[maybe_unused]] int res = 0;
    static std::chrono::time_point start = std::chrono::high_resolution_clock::now();
    // ckptTimePrintEvent(event, data);
    switch (event) {
        case DMTCP_EVENT_INIT: {
            setbuf(stdout, NULL);
            setbuf(stderr, NULL);
        } break;
        case DMTCP_EVENT_PRECHECKPOINT: {
            // debug_info("DMTCP_EVENT_PRECHECKPOINT");
            start = std::chrono::high_resolution_clock::now();
            // debug_info("DMTCP_EVENT_PRECHECKPOINT");

        } break;
        case DMTCP_EVENT_POSTCHECKPOINT: {
            // debug_info("DMTCP_EVENT_POSTCHECKPOINT");
            dmtcp_local_barrier("CKPT_TIME_PLUGIN:postcheckpoint1");
            double elapsed_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(
                                         std::chrono::high_resolution_clock::now() - start)
                                         .count();
            printf("Time taken in ckpt: %.6f seconds\n", elapsed_seconds);
            fflush(stdout);
            dmtcp_local_barrier("CKPT_TIME_PLUGIN:postcheckpoint2");
            // sleep(1);
            // debug_info("DMTCP_EVENT_POSTCHECKPOINT end = " << res);
        } break;
        default:
            break;
    }
}

DmtcpPluginDescriptor_t ckpt_time_plugin = {
    DMTCP_PLUGIN_API_VERSION, PACKAGE_VERSION,    "CKPT time",         "CKPT time",
    "CKPT time@gmail.todo",   "CKPT TIME plugin", ckpt_time_event_hook};

DMTCP_DECL_PLUGIN(ckpt_time_plugin);
