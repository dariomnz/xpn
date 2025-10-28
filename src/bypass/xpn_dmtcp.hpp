
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
#include <chrono>
#include <cstdlib>

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include <cstring>
#include <filesystem>
#include <string>
#include <thread>

#include "base_cpp/allocator.hpp"
#include "base_cpp/debug.hpp"
#include "config.h"
#include "dmtcp.h"
#include "xpn.h"

class xpn_dmtcp {
   public:
    static inline void update_restarts() {
        auto &instance = XPN::ArenaAllocatorStorage::instance();
        int numCheckpoints = 0;
        int numRestarts = 0;
        dmtcp_get_local_status(&numCheckpoints, &numRestarts);
        debug_info("savedRestarts " << instance.m_savedRestarts << " numRestarts " << numRestarts);
        instance.m_savedRestarts = numRestarts;
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
};