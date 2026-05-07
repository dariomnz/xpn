
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
#include "adaptative_compressor.hpp"

#include "base_cpp/debug.hpp"
#include "base_cpp/xpn_env.hpp"

namespace XPN {

AdaptiveCompressorStats::~AdaptiveCompressorStats() {
    // UNCOMMENT to print stats
    // print(*this);
}

#define DIV_0(up, down) ((down) == 0 ? 0 : ((up) / (down)))

void AdaptiveCompressorStats::add_metric(uint64_t original_size, uint64_t total_us, uint64_t net_us, uint64_t rw_us) {
    m_original_size += original_size;
    m_total_us += total_us;
    m_net_us += net_us;
    m_rw_us += rw_us;
    m_count++;
    m_times_no_comp++;
}

void AdaptiveCompressorStats::add_metric_comp(uint64_t original_size, uint64_t compressed_size, uint64_t total_us,
                                              uint64_t net_us, uint64_t rw_us, uint64_t comp_us, uint64_t decomp_us,
                                              uint64_t ratio_x_1000) {
    m_original_size += original_size;
    m_compressed_size += compressed_size;
    m_total_comp_us += total_us;
    m_net_us += net_us;
    m_rw_us += rw_us;
    m_comp_us += comp_us;
    m_decomp_us += decomp_us;
    m_ratio_x_1000 += ratio_x_1000;
    m_count++;
    m_count_comp++;
    m_times_comp++;
}
std::ostream& operator<<(std::ostream& os, AdaptiveCompressorStats& stats) {
    const uint64_t count = (stats.m_count == 0 ? 1 : stats.m_count);
    const uint64_t count_comp = (stats.m_count_comp == 0 ? 1 : stats.m_count_comp);
    const uint64_t original_size = stats.m_original_size / count;
    const uint64_t compressed_size = stats.m_compressed_size / count_comp;
    const uint64_t total_us = stats.m_total_us / count;
    const uint64_t total_comp_us = stats.m_total_comp_us / count;
    const uint64_t net_us = stats.m_net_us / count;
    const uint64_t rw_us = stats.m_rw_us / count;
    const uint64_t comp_us = stats.m_comp_us / count_comp;
    const uint64_t decomp_us = stats.m_decomp_us / count_comp;
    const uint64_t ratio_x_1000 = stats.m_ratio_x_1000 / count_comp;
    const double ratio = static_cast<double>(ratio_x_1000) / 1000.0;
    const uint64_t total_times =
        (stats.m_times_comp + stats.m_times_no_comp == 0 ? 1 : stats.m_times_comp + stats.m_times_no_comp);
    const double perc_comp = static_cast<double>(stats.m_times_comp) / total_times;
    const double perc_no_comp = static_cast<double>(stats.m_times_no_comp) / total_times;

    const double factor = 1000000.0 / (1024.0 * 1024.0);
    const double mbps_total =
        DIV_0((static_cast<double>(stats.m_original_size) * perc_no_comp * factor), stats.m_total_us);
    const double mbps_total_comp =
        DIV_0((static_cast<double>(stats.m_original_size) * perc_comp * factor), stats.m_total_comp_us);

    os << (stats.m_type == AdaptiveCompressor::type_t::Read ? "Read" : "Write");
    os << " times " << total_times << " comp " << stats.m_times_comp << " (" << static_cast<uint32_t>(perc_comp * 100.0)
       << "%)";
    os << " nocomp " << stats.m_times_no_comp << " (" << static_cast<uint32_t>(perc_no_comp * 100.0) << "%)";
    os << " original_size " << original_size << " (" << stats.m_original_size << ")";
    os << " compressed_size " << compressed_size << " (" << stats.m_compressed_size << ")";
    os << " total_us " << total_us << " (" << stats.m_total_us << ") (" << mbps_total << "MB/s)";
    os << " total_comp_us " << total_comp_us << " (" << stats.m_total_comp_us << ") (" << mbps_total_comp << "MB/s)";
    os << " net_us " << net_us << " (" << stats.m_net_us << ")";
    os << " rw_us " << rw_us << " (" << stats.m_rw_us << ")";
    os << " comp_us " << comp_us << " (" << stats.m_comp_us << ")";
    os << " decomp_us " << decomp_us << " (" << stats.m_decomp_us << ")";
    os << " ratio " << ratio;
    return os;
}

inline void apply_metric(double& target, double current_value, double min_alpha, int64_t count = -1) {
    if (current_value <= 0) return;

    double alpha = min_alpha;
    if (count > 0) {
        alpha = std::max(min_alpha, 1.0 / (1.0 + count));
    }

    if (target <= 0.0) {
        target = current_value;
    } else {
        target = (current_value * alpha) + (target * (1.0 - alpha));
    }
}

bool AdaptiveCompressor::should_compress(uint64_t size) {
    auto& env = xpn_env::get_instance();
    if (env.xpn_net_compression == 0) return false;
    if (size < ignore_compress_size) return false;

    // Forced Learning / Exploration Phase
    // If metrics are missing or counter hits interval, force compression to refresh data.

    if ((m_counter++ >= m_exploration_interval) && env.xpn_net_compression != 3) {
        m_counter = 0;
        // m_count_want_to_change = 0;
        // m_is_compressing_active = !m_is_compressing_active;
        // return m_is_compressing_active;
        return true;
        // return !m_is_compressing_active;
    }
    if (m_started_count < m_started_count_limit) {
        if (m_started_count < (m_started_count_limit / 2)) {
            return true;
        } else {
            return false;
        }
    }

    const double t_original = get_t_no_comp(size);
    const double t_teoric = get_t_comp(size);

    const bool old_state = m_is_compressing_active;
    enum class want_change_t {
        No,
        To_comp,
        To_no_comp,
    };
    want_change_t want_change = want_change_t::No;
    if (!old_state) {
        // if (m_avg_client_traker.get_mbps() < m_avg_client_traker_comp.get_mbps()) {
        //     want_change = want_change_t::To_comp;
        // }
        if (t_teoric < t_original) {
            want_change = want_change_t::To_comp;
            debug_info("Want change to compression");
            // m_is_compressing_active = true;
        }
    } else {
        // if (m_avg_client_traker.get_mbps() > m_avg_client_traker_comp.get_mbps()) {
        //     want_change = want_change_t::To_no_comp;
        // }
        if (t_teoric > t_original) {
            want_change = want_change_t::To_no_comp;
            debug_info("Want change to no compression");
            // m_is_compressing_active = false;
        }
    }

    if (want_change == want_change_t::No) {
        m_count_want_to_change = 0;
    } else {
        m_count_want_to_change++;
        if (m_count_want_to_change > m_count_want_to_change_limit) {
            if (want_change == want_change_t::To_comp) {
                m_is_compressing_active = true;
                m_count_want_to_change = 0;
            } else if (want_change == want_change_t::To_no_comp) {
                m_is_compressing_active = false;
                m_count_want_to_change = 0;
            }
        }
    }

    // Modes that always compress, never, and one each
    if (env.xpn_net_compression == 2) m_is_compressing_active = true;
    if (env.xpn_net_compression == 3) m_is_compressing_active = false;
    if (env.xpn_net_compression == 4) {
        m_is_compressing_active = m_flag_comp % 2 == 0;
        m_flag_comp++;
    }

#ifdef DEBUG
    const double size_mb = (size / (1024.0 * 1024.0));
    const double t_total = DIV_0(size_mb, m_mbps_avg_total);
    const double t_total_avg = DIV_0(size_mb, m_avg_client_traker.get_mbps());
    std::stringstream ss;
    ss << std::fixed << std::setprecision(4) << m_debug_name << "\n";

    // Input Data
    ss << "  [INPUT] Size: " << format_bytes(size)
       << " bytes | Ignore Threshold: " << format_bytes(ignore_compress_size) << "\n";
    // Metrics
    ss << "  [METRICS] Total Avg: " << m_avg_client_traker.get_mbps() << " MB/s"
       << " | Total: " << m_mbps_avg_total << " MB/s"
       << " | Net: " << m_mbps_avg_net << " MB/s"
       << " | RW: " << m_mbps_avg_rw << " MB/s"
       << " | Comp: " << m_mbps_avg_comp << " MB/s"
       << " | Decomp: " << m_mbps_avg_decomp << " MB/s"
       << " | Ratio: " << m_avg_comp_ratio << "\n";
    // Model Analysis
    ss << "  [MODEL] T_total: " << (t_total * 1000) << "ms T_total_avg: " << (t_total_avg * 1000) << "ms\n";
    // Decision
    ss << "  [DECISION] " << (old_state ? "COMP" : "RAW") << " -> " << (m_is_compressing_active ? "COMP" : "RAW")
       << (old_state != m_is_compressing_active ? " *** SWITCHED ***" : " (STABLE)");
    debug_info(ss.str());
#endif

    return m_is_compressing_active;
}

double AdaptiveCompressor::get_t_no_comp(uint64_t size) {
    // Convert to Mb
    const double size_mb = (size / (1024.0 * 1024.0));

    // Total measure -> client track
    // Net measure   -> client net
    // const double aprox_net = (m_avg_client_traker.get_mbps() * m_mbps_avg_net) / m_mbps_avg_total;
    // const double aprox_num_clients = m_mbps_avg_server / m_avg_client_traker.get_mbps();

    // const double aprox_num_clients_per_server = aprox_num_clients / m_num_servers;

    const double num_clients_per_server = static_cast<double>(m_num_clients) / m_num_servers;
    // num_clients = num_server * num_clients_per_node

    const double aprox_net =
        m_mbps_avg_net / (num_clients_per_server * xpn_env::get_instance().xpn_compression_net_multiplier);
    // print("Cli/ser " << num_clients_per_server << " Aprox_net " << m_mbps_avg_net << " / " << aprox_net);
    // const double aprox_net = m_mbps_avg_net / aprox_num_clients;
    // const double aprox_net = m_mbps_avg_net / m_num_servers;

    //                  size          size
    // t_original = ------------ + -----------
    //              mbps_avg_net   mbps_avg_rw
    const double t_net_measure = DIV_0(size_mb, aprox_net);
    // const double t_net_avg = DIV_0(size_mb, m_mbps_avg_net_client);
    const double t_rw_measure = DIV_0(size_mb, m_mbps_avg_rw);
    const double t_original = t_net_measure + t_rw_measure;
    // const double t_original_avg = t_net_avg + t_rw_measure;
    // const double t_original_avg = DIV_0(size_mb, m_mbps_avg_client);
    // const double t_original = (t_original_measure * percentage_measure) + (t_original_avg * (1.0 -
    // percentage_measure));

#ifdef DEBUG
    std::stringstream ss;
    ss << std::fixed << std::setprecision(4) << m_debug_name << "\n";
    // Input Data
    ss << "  [INPUT] Size: " << format_bytes(size) << " bytes | Clients: " << m_num_clients
       << " Cli per srv: " << num_clients_per_server << " | Net: " << m_mbps_avg_net << " Aprox net: " << aprox_net
       << " * " << xpn_env::get_instance().xpn_compression_net_multiplier << " = " << aprox_net << "\n";
    // Model Analysis
    ss << "  [MODEL] T_Orig: " << (t_net_measure * 1000) << " + " << (t_rw_measure * 1000) << " = "
       << (t_original * 1000) << "ms";
    // ss << "          T_Orig_avg: " << (t_original_avg * 1000) << "ms\n";
    // ss << "          T_Orig: (" << (t_original_measure * 1000) << " * " << percentage_measure << ") + ("
    //    << (t_original_avg * 1000) << " * " << (1 - percentage_measure) << ") = " << (t_original * 1000) << "ms";
    debug_info(ss.str());
#endif

    return t_original;
}

double AdaptiveCompressor::get_t_comp(uint64_t size) {
    // Convert to Mb
    const double size_mb = (size / (1024.0 * 1024.0));

    // Total measure -> client track
    // Net measure   -> client net
    // const double aprox_net = (m_avg_client_traker.get_mbps() * m_mbps_avg_net) / m_mbps_avg_total;
    // const double aprox_num_clients = m_mbps_avg_server / m_avg_client_traker.get_mbps();

    // const double aprox_num_clients_per_server = aprox_num_clients / m_num_servers;

    // // const double aprox_net = m_mbps_avg_net / aprox_num_clients;
    // const double aprox_net = m_mbps_avg_net / m_num_clients;
    // const double aprox_net = m_mbps_avg_net / m_num_servers;

    const double num_clients_per_server = static_cast<double>(m_num_clients) / m_num_servers;
    // num_clients = num_server * num_clients_per_node

    const double aprox_net =
        m_mbps_avg_net / (num_clients_per_server * xpn_env::get_instance().xpn_compression_net_multiplier);

    //                size         size*ratio       size            size
    // t_teoric = ------------- + ------------ + ----------- + ---------------
    //            mbps_avg_comp   mbps_avg_net   mbps_avg_rw   mbps_avg_decomp
    const double t_rw_measure = DIV_0(size_mb, m_mbps_avg_rw);
    const double t_comp_measure = DIV_0(size_mb, m_mbps_avg_comp);
    const double t_net_ratio_measure = DIV_0((size_mb * m_avg_comp_ratio), aprox_net);
    const double t_decomp_measure = DIV_0(size_mb, m_mbps_avg_decomp);
    const double t_teoric = t_comp_measure + t_net_ratio_measure + t_rw_measure + t_decomp_measure;

    // const double t_net_ratio_avg = DIV_0((size_mb * m_avg_comp_ratio), m_mbps_avg_net_client);
    // const double t_teoric_avg = t_comp_measure + t_net_ratio_avg + t_rw_measure + t_decomp_measure;
    // const double t_teoric_avg = DIV_0(size_mb, m_mbps_avg_client);
    // const double t_teoric = (t_teoric_measure * percentage_measure) + (t_teoric_avg * (1.0 - percentage_measure));

#ifdef DEBUG
    std::stringstream ss;
    ss << std::fixed << std::setprecision(4) << m_debug_name << "\n";
    // Input Data
    ss << "  [INPUT] Size: " << format_bytes(size) << " bytes | Clients: " << m_num_clients
       << " Cli per srv: " << num_clients_per_server << " | Net: " << m_mbps_avg_net << " Aprox net: " << aprox_net
       << " * " << xpn_env::get_instance().xpn_compression_net_multiplier << " = " << aprox_net << "\n";
    // Model Analysis
    ss << "  [MODEL] T_Teo: " << (t_comp_measure * 1000) << " + " << (t_net_ratio_measure * 1000) << " + "
       << (t_rw_measure * 1000) << " + " << (t_decomp_measure * 1000) << " = " << (t_teoric * 1000) << "ms";
    // ss << "          T_Teo_avg:  " << (t_teoric_avg * 1000) << "ms\n";
    // ss << "          T_Teo: (" << (t_teoric_measure * 1000) << " * " << percentage_measure << ") + ("
    //    << (t_teoric_avg * 1000) << " * " << (1 - percentage_measure) << ") = " << (t_teoric * 1000) << "ms";
    debug_info(ss.str());
#endif

    return t_teoric;
}

void AdaptiveCompressor::update_metrics(uint64_t size, uint32_t num_clients, std::chrono::microseconds total_duration,
                                        std::chrono::microseconds rw_duration) {
    update_metrics_internal(update_type_t::NO_COMP, size, size, num_clients, total_duration,
                            std::chrono::microseconds(0), rw_duration, std::chrono::microseconds(0),
                            std::chrono::microseconds(0));
    m_avg_client_traker.add_sample(size);
}

void AdaptiveCompressor::update_metrics_comp(uint64_t original_size, uint64_t compressed_size, uint32_t num_clients,
                                             std::chrono::microseconds total_duration,
                                             std::chrono::microseconds rw_duration,
                                             std::chrono::microseconds comp_duration,
                                             std::chrono::microseconds decomp_duration) {
    update_metrics_internal(update_type_t::COMP, original_size, compressed_size, num_clients,
                            std::chrono::microseconds(0), total_duration, rw_duration, comp_duration, decomp_duration);
    m_avg_client_traker.add_sample(original_size);
}

void AdaptiveCompressor::update_metrics_internal(update_type_t type, uint64_t original_size, uint64_t compressed_size,
                                                 uint32_t num_clients, std::chrono::microseconds total_duration,
                                                 std::chrono::microseconds total_comp_duration,
                                                 std::chrono::microseconds rw_duration,
                                                 std::chrono::microseconds comp_duration,
                                                 std::chrono::microseconds decomp_duration) {
    m_started_count++;

    // Update server mbps
    m_num_clients = num_clients;

    const double original_size_mb = original_size / (1024.0 * 1024.0);
    const double compressed_size_mb = compressed_size / (1024.0 * 1024.0);
    // Update total comp
    double total_mbps = 0;
    std::chrono::microseconds current_total_duration{0};
    if (total_duration.count() > 0) {
        current_total_duration = total_duration;
        total_mbps = original_size_mb / (total_duration.count() / 1e6);
        apply_metric(m_mbps_avg_total, total_mbps, m_alpha);
    }
    // Update total comp
    double total_comp_mbps = 0;
    if (total_comp_duration.count() > 0) {
        current_total_duration = total_comp_duration;
        total_comp_mbps = original_size_mb / (total_comp_duration.count() / 1e6);
        apply_metric(m_mbps_avg_total, total_comp_mbps, m_alpha);
    }
    // Update net server
    std::chrono::microseconds current_total_avg_duration{0};
    if (total_duration.count() > 0) {
        current_total_avg_duration = std::chrono::microseconds{
            static_cast<uint64_t>((compressed_size_mb / m_avg_client_traker.get_mbps()) * 1e6)};
    }
    if (total_comp_duration.count() > 0) {
        current_total_avg_duration = std::chrono::microseconds{
            static_cast<uint64_t>((compressed_size_mb / m_avg_client_traker.get_mbps()) * 1e6)};
    }

    // Update Network Speed
    double net_mbps = 0;
    std::chrono::microseconds net_duration = current_total_duration;
    if (net_duration.count() > 0) {
        if (comp_duration.count() > 0) {
            net_duration -= comp_duration;
        }
        if (rw_duration.count() > 0) {
            net_duration -= rw_duration;
        }
        if (decomp_duration.count() > 0) {
            net_duration -= decomp_duration;
        }
        if (net_duration.count() <= 0) {
            net_duration = std::chrono::microseconds(1);
        }

        net_mbps = compressed_size_mb / (net_duration.count() / 1e6);
        apply_metric(m_mbps_avg_net, net_mbps, m_alpha);
    }
    // double client_avg_net_mbps = 0;
    // std::chrono::microseconds avg_net_duration = current_total_avg_duration;
    // if (avg_net_duration.count() > 0) {
    //     if (comp_duration.count() > 0) {
    //         avg_net_duration -= comp_duration;
    //     }
    //     if (rw_duration.count() > 0) {
    //         avg_net_duration -= rw_duration;
    //     }
    //     if (decomp_duration.count() > 0) {
    //         avg_net_duration -= decomp_duration;
    //     }
    //     if (avg_net_duration.count() > 0) {
    //         client_avg_net_mbps = compressed_size_mb / (avg_net_duration.count() / 1e6);
    //         apply_metric(m_mbps_avg_net_client, client_avg_net_mbps, m_alpha);
    //     }
    // }
    // Update Disk Speed
    double rw_mbps = 0;
    if (rw_duration.count() < 0) {
        rw_duration = std::chrono::microseconds(1);
    }
    if (rw_duration.count() > 0) {
        rw_mbps = original_size_mb / (rw_duration.count() / 1e6);
        apply_metric(m_mbps_avg_rw, rw_mbps, m_alpha);
    }

    // Update Compression Speed and Ratio
    double comp_mbps = 0;
    double current_ratio = 0;
    if (comp_duration.count() < 0) {
        comp_duration = std::chrono::microseconds(1);
    }
    if (comp_duration.count() > 0) {
        comp_mbps = original_size_mb / (comp_duration.count() / 1e6);
        apply_metric(m_mbps_avg_comp, comp_mbps, m_alpha);

        current_ratio = static_cast<double>(compressed_size) / original_size;
        apply_metric(m_avg_comp_ratio, current_ratio, m_alpha);
    }

    // Update Decompression Speed
    double decomp_mbps = 0;
    if (decomp_duration.count() < 0) {
        decomp_duration = std::chrono::microseconds(1);
    }
    if (decomp_duration.count() > 0) {
        decomp_mbps = original_size_mb / (decomp_duration.count() / 1e6);
        apply_metric(m_mbps_avg_decomp, decomp_mbps, m_alpha);
    }
    AdaptiveCompressorStats* instance;
    if (m_type == type_t::Read) {
        instance = &AdaptiveCompressorStats::get_instance_read();
    } else {
        instance = &AdaptiveCompressorStats::get_instance_write();
    }
    if (type == update_type_t::NO_COMP) {
        instance->add_metric(original_size, current_total_duration.count(), net_duration.count(), rw_duration.count());
    } else {
        instance->add_metric_comp(original_size, compressed_size, current_total_duration.count(), net_duration.count(),
                                  rw_duration.count(), comp_duration.count(), decomp_duration.count(),
                                  current_ratio * 1000);
    }
    // add_window_metrics(net_mbps, rw_mbps);

#ifdef DEBUG
    // Average avg = get_window_avg();
    const double aprox_net = m_mbps_avg_net / m_num_servers;
    std::stringstream ss;
    ss << std::fixed << std::setprecision(3) << m_debug_name;
    // Metric Update
    ss << "\n  [METRIC UPDATE] Size: " << format_bytes(original_size) << " -> " << format_bytes(compressed_size)
       << " bytes (Inst. Ratio: " << current_ratio << ")";

    // Durations
    ss << "\n  [DURATIONS] Client avg: " << current_total_avg_duration.count()
       << "us | Total: " << total_duration.count() << "us | Total comp: " << total_comp_duration.count()
       << "us | Net: " << net_duration.count() << "us | RW: " << rw_duration.count()
       << "us | Comp: " << comp_duration.count() << "us | Decomp: " << decomp_duration.count() << "us";

    // Instant Speeds
    ss << "\n  [INSTANT SPEEDS] Client: " << m_avg_client_traker.get_mbps() << " MB/s | Total: " << total_mbps
       << " MB/s | Total comp: " << total_comp_mbps << " MB/s | Net: " << net_mbps << " MB/s | RW: " << rw_mbps
       << " MB/s | Comp: " << comp_mbps << " MB/s | Decomp: " << decomp_mbps << " MB/s | Ratio: " << current_ratio;

    // Moving Averages
    ss << "\n  [MOVING AVERAGES] Client: " << m_avg_client_traker.get_mbps() << " | Server: " << m_mbps_avg_server
       << " | Total: " << m_mbps_avg_total << " | Net_Avg: " << m_mbps_avg_net << " | Net_aprox: " << aprox_net
       << " | RW_Avg: " << m_mbps_avg_rw << " | Comp_Avg: " << m_mbps_avg_comp << " | Decomp_Avg: " << m_mbps_avg_decomp
       << " | Ratio_Avg: " << m_avg_comp_ratio;
    debug_info(ss.str());
#endif
}
}  // namespace XPN
