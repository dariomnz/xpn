
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

#include <lz4.h>

#include <chrono>
#include <cstdint>
#include <random>
#include <vector>

#include "base_cpp/debug.hpp"
#include "base_cpp/xpn_env.hpp"
#include "xpn_server/xpn_server_params.hpp"

namespace XPN {
class AdaptiveCompressor {
   private:
    const std::string m_debug_name;
    // Metrics initialized at -1 to trigger the initial learning phase
    bool m_started = false;
    double m_mbps_avg_net = -1.0;
    double m_mbps_avg_rw = -1.0;
    double m_mbps_avg_comp = -1.0;
    double m_mbps_avg_decomp = -1.0;
    double m_avg_comp_ratio = -1.0;

    const double m_alpha = 0.2;                       // Smoothing factor for the Moving Average
    bool m_is_compressing_active = false;             // State for hysteresis

    uint64_t m_counter = 0;                           // Exploration counter
    const uint64_t m_exploration_interval = 100;
    const uint64_t ignore_compress_size = 16 * 1024;  // 16 KB

   public:
    AdaptiveCompressor(const std::string debug_name) : m_debug_name(debug_name) {}

    bool should_compress(uint64_t size);

    void update_metrics(uint64_t size, std::chrono::microseconds net_duration, std::chrono::microseconds rw_duration);

    void update_metrics_comp(uint64_t original_size, uint64_t compressed_size, std::chrono::microseconds net_duration,
                             std::chrono::microseconds rw_duration, std::chrono::microseconds comp_duration,
                             std::chrono::microseconds decomp_duration);
};
}  // namespace XPN