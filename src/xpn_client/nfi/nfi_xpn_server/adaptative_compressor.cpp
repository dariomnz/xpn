
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

#include "adaptative_compressor.hpp"

namespace XPN {

inline void apply_metric(double& target, double current_value, double alpha) {
    if (current_value <= 0) return;
    if (target < 0) {
        target = current_value;
    } else {
        target = (current_value * alpha) + (target * (1.0 - alpha));
    }
}
bool AdaptiveCompressor::should_compress(uint64_t size) {
    auto& env = xpn_env::get_instance();
    if (env.xpn_compression == 0) return false;
    if (env.xpn_compression == 2) return true;
    if (size < ignore_compress_size) return false;

    // Forced Learning / Exploration Phase
    // If metrics are missing or counter hits interval, force compression to refresh data.
    if (!m_started || m_counter++ >= m_exploration_interval) {
        m_counter = 0;
        return true;
    }

    // Convert to Mb
    const double size_mb = (size / (1024.0 * 1024.0));

    //                  size           size
    // t_original = ------------ + -----------
    //              mbps_avg_net   mbps_avg_rw
    double t_original = (size_mb / m_mbps_avg_net) + (size_mb / m_mbps_avg_rw);

    //                size         size*ratio    size*ratio         size
    // t_teoric = ------------- + ------------ + ----------- + ---------------
    //            mbps_avg_comp   mbps_avg_net   mbps_avg_rw   mbps_avg_decomp
    double t_teoric = (size_mb / m_mbps_avg_comp) + (size_mb * m_avg_comp_ratio / m_mbps_avg_net) +
                      (size_mb * m_avg_comp_ratio / m_mbps_avg_rw) + (size_mb / m_mbps_avg_decomp);

    // 10% margin to prevent state flapping
    const bool old_state = m_is_compressing_active;
    const double lower_bound = t_original * 0.9;
    const double upper_bound = t_original * 1.1;
    if (!old_state) {
        if (t_teoric < lower_bound) m_is_compressing_active = true;
    } else {
        if (t_teoric > upper_bound) m_is_compressing_active = false;
    }
    debug_info("" << std::fixed << std::setprecision(4) << m_debug_name << "\n  [INPUT] Size: " << format_bytes(size)
                  << " bytes | Ignore Threshold: " << format_bytes(ignore_compress_size)
                  << "\n  [METRICS] Net: " << m_mbps_avg_net << " MB/s | RW: " << m_mbps_avg_rw << " MB/s"
                  << " | Comp: " << m_mbps_avg_comp << " MB/s | Decomp: " << m_mbps_avg_decomp << " MB/s"
                  << " | Ratio: " << m_avg_comp_ratio << "\n  [MODEL] T_Orig: " << (t_original * 1000)
                  << "ms (Bounds: " << (lower_bound * 1000) << " - " << (upper_bound * 1000) << ")"
                  << "\n          T_Teo:  " << (t_teoric * 1000) << "ms"
                  << "\n  [DECISION] " << (old_state ? "COMP" : "RAW") << " -> "
                  << (m_is_compressing_active ? "COMP" : "RAW")
                  << (old_state != m_is_compressing_active ? " *** SWITCHED ***" : " (STABLE)"));

    return m_is_compressing_active;
}

void AdaptiveCompressor::update_metrics(uint64_t size, std::chrono::microseconds net_duration,
                                        std::chrono::microseconds rw_duration) {
    update_metrics_comp(size, size, net_duration, rw_duration, std::chrono::microseconds(0),
                        std::chrono::microseconds(0));
}

void AdaptiveCompressor::update_metrics_comp(uint64_t original_size, uint64_t compressed_size,
                                             std::chrono::microseconds net_duration,
                                             std::chrono::microseconds rw_duration,
                                             std::chrono::microseconds comp_duration,
                                             std::chrono::microseconds decomp_duration) {
    m_started = true;
    // Update Network Speed
    double net_mbps = 0;
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

        net_mbps = (compressed_size / (1024.0 * 1024.0)) / (net_duration.count() / 1e6);
        apply_metric(m_mbps_avg_net, net_mbps, m_alpha);
    }
    // Update Disk Speed
    double rw_mbps = 0;
    if (rw_duration.count() < 0) {
        rw_duration = std::chrono::microseconds(1);
    }
    if (rw_duration.count() > 0) {
        rw_mbps = (original_size / (1024.0 * 1024.0)) / (rw_duration.count() / 1e6);
        apply_metric(m_mbps_avg_rw, rw_mbps, m_alpha);
    }

    // Update Compression Speed and Ratio
    double comp_mbps = 0;
    double current_ratio = 0;
    if (comp_duration.count() < 0) {
        comp_duration = std::chrono::microseconds(1);
    }
    if (comp_duration.count() > 0) {
        comp_mbps = (original_size / (1024.0 * 1024.0)) / (comp_duration.count() / 1e6);
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
        decomp_mbps = (original_size / (1024.0 * 1024.0)) / (decomp_duration.count() / 1e6);
        apply_metric(m_mbps_avg_decomp, decomp_mbps, m_alpha);
    }
    debug_info("" << std::fixed << std::setprecision(3) << m_debug_name
                  << "\n  [METRIC UPDATE] Size: " << format_bytes(original_size) << " -> "
                  << format_bytes(compressed_size) << " bytes (Inst. Ratio: " << current_ratio << ")"
                  << "\n  [DURATIONS] Net: " << net_duration.count() << "us | RW: " << rw_duration.count()
                  << "us | Comp: " << comp_duration.count() << "us | Decomp: " << decomp_duration.count() << "us"
                  << "\n  [INSTANT SPEEDS] Net: " << net_mbps << " MB/s | RW: " << rw_mbps
                  << " MB/s | Comp: " << comp_mbps << " MB/s | Decomp: " << decomp_mbps << " MB/s"
                  << "\n  [MOVING AVERAGES] Net_Avg: " << m_mbps_avg_net << " | RW_Avg: " << m_mbps_avg_rw
                  << " | Comp_Avg: " << m_mbps_avg_comp << " | Ratio_Avg: " << m_avg_comp_ratio);
}
}  // namespace XPN
