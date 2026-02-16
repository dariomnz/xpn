
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
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>

#include "base_cpp/debug.hpp"
#include "dmtcp.h"
#include "xpn.h"

struct scope_disable_ckpt {
    scope_disable_ckpt() { DMTCP_PLUGIN_DISABLE_CKPT(); }
    ~scope_disable_ckpt() { DMTCP_PLUGIN_ENABLE_CKPT(); }
};

class xpn_dmtcp {
   public:
    static xpn_dmtcp& instance() {
        static xpn_dmtcp inst;
        return inst;
    }

    static inline void update_restarts() {
        auto& instan = instance();
        int numCheckpoints = 0;
        int numRestarts = 0;
        dmtcp_get_local_status(&numCheckpoints, &numRestarts);
        debug_info("savedRestarts " << instan.m_savedRestarts << " numRestarts " << numRestarts);
        instan.m_savedRestarts = numRestarts;
    }

    // static inline bool in_restart() {
    //     auto &instance = XPN::ArenaAllocatorStorage::instance();
    //     int numCheckpoints = 0;
    //     int numRestarts = 0;
    //     dmtcp_get_local_status(&numCheckpoints, &numRestarts);

    //     bool ret = numRestarts != instance.m_savedRestarts;
    //     debug_info("savedRestarts " << instance.m_savedRestarts << " numRestarts " << numRestarts << " ret "
    //                                 << (ret ? "true" : "false"));
    //     return ret;
    // }

   public:
    bool m_inCkpt = false;
    bool m_disableAlloc = false;
    int m_savedRestarts = 0;
};