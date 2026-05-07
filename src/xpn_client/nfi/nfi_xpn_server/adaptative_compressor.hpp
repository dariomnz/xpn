
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
#include "base_cpp/stat_tracker.hpp"
#include "base_cpp/xpn_env.hpp"
#include "xpn_server/xpn_server_params.hpp"

namespace XPN {

class AdaptiveCompressor {
   public:
    enum class type_t { Read, Write };

   private:
    type_t m_type;
    const std::string m_debug_name;
    const uint32_t m_num_servers;

    int m_started_count = 0;
    const int m_started_count_limit = 0;

    StatTraker m_avg_client_traker;

    // The measure by the server
    uint32_t m_num_clients;

    // The measure by the client
    double m_mbps_avg_total = 0.0;
    double m_mbps_avg_net = 0.0;
    uint64_t m_count_avg_net = 0;

    // Measurements
    double m_mbps_avg_rw = 0.0;
    double m_mbps_avg_comp = 0.0;
    double m_mbps_avg_decomp = 0.0;
    double m_avg_comp_ratio = 1.0;

    uint64_t m_flag_comp = 0;              // for xpn_conpression 4 one each

    const double m_alpha = 0.01;           // Smoothing factor for the Moving Average
    bool m_is_compressing_active = false;  // State for hysteresis

    int m_count_want_to_change_limit = 1;
    int m_count_want_to_change = 0;

    uint64_t m_counter = 0;                           // Exploration counter
    const uint64_t m_exploration_interval = 100;
    const uint64_t ignore_compress_size = 16 * 1024;  // 16 KB

   public:
    AdaptiveCompressor(type_t type, const std::string debug_name, uint32_t num_servers)
        : m_type(type), m_debug_name(debug_name), m_num_servers(num_servers) {}

    bool should_compress(uint64_t size);

    void update_metrics(uint64_t size, uint32_t num_clients, std::chrono::microseconds total_duration,
                        std::chrono::microseconds rw_duration);

    void update_metrics_comp(uint64_t original_size, uint64_t compressed_size, uint32_t num_clients,
                             std::chrono::microseconds total_duration, std::chrono::microseconds rw_duration,
                             std::chrono::microseconds comp_duration, std::chrono::microseconds decomp_duration);

   private:
    enum class update_type_t {
        COMP,
        NO_COMP,
    };
    void update_metrics_internal(update_type_t type, uint64_t original_size, uint64_t compressed_size,
                                 uint32_t num_clients, std::chrono::microseconds total_duration,
                                 std::chrono::microseconds total_comp_duration, std::chrono::microseconds rw_duration,
                                 std::chrono::microseconds comp_duration, std::chrono::microseconds decomp_duration);
    double get_t_no_comp(uint64_t size);
    double get_t_comp(uint64_t size);
};

class AdaptiveCompressorStats {
    AdaptiveCompressor::type_t m_type;
    uint64_t m_times_no_comp;
    uint64_t m_times_comp;
    uint64_t m_original_size;
    uint64_t m_compressed_size;
    uint64_t m_total_us;
    uint64_t m_total_comp_us;
    uint64_t m_net_us;
    uint64_t m_rw_us;
    uint64_t m_comp_us;
    uint64_t m_decomp_us;
    uint64_t m_ratio_x_1000;
    uint64_t m_count;
    uint64_t m_count_comp;

   public:
    AdaptiveCompressorStats(AdaptiveCompressor::type_t type) : m_type(type) {}
    ~AdaptiveCompressorStats();

    void add_metric(uint64_t original_size, uint64_t total_us, uint64_t net_us, uint64_t rw_us);
    void add_metric_comp(uint64_t original_size, uint64_t compressed_size, uint64_t total_us, uint64_t net_us,
                         uint64_t rw_us, uint64_t comp_us, uint64_t decomp_us, uint64_t ratio_x_1000);
    friend std::ostream& operator<<(std::ostream& os, AdaptiveCompressorStats& stats);

    static AdaptiveCompressorStats& get_instance_read() {
        static AdaptiveCompressorStats instance{AdaptiveCompressor::type_t::Read};
        return instance;
    }
    static AdaptiveCompressorStats& get_instance_write() {
        static AdaptiveCompressorStats instance{AdaptiveCompressor::type_t::Write};
        return instance;
    }
};

}  // namespace XPN