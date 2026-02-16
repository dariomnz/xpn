
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
#include <random>
#include <vector>

#include "base_cpp/xpn_env.hpp"
#include "base_cpp/debug.hpp"
#include "xpn_server/xpn_server_params.hpp"

namespace XPN {

class AdaptiveCompressor {
   private:
    double m_mbps_avg_write = 5000.0;            // Empezamos asumiendo red muy rápida (ej. 5Gbps)
    double m_mbps_avg_read = 5000.0;             // Empezamos asumiendo red muy rápida (ej. 5Gbps)
    const double m_alpha = 0.2;                  // Factor de suavizado para la media móvil
    double m_cpu_limit_mbps = -1.0;
    bool m_currently_compressing_write = false;  // Estado para la histéresis
    bool m_currently_compressing_read = false;   // Estado para la histéresis

    const uint64_t ignore_compress_size = 16 * 1024; // 16 KB 

   public:
    struct Result {
        const char* data;
        uint64_t size;
        bool was_compressed;
    };

    // AdaptiveCompressor() {
    //     if (xpn_env::get_instance().xpn_compression != 0) m_cpu_limit_mbps = calibrate(MAX_BUFFER_SIZE);
    // }

    static double calibrate(uint64_t test_size) {
        static double calibrated_result = -1;
        if (calibrated_result > 0) return calibrated_result;
        // 1. Preparar datos aleatorios pero "comprimibles" (mezcla de random y repetidos)
        std::vector<char> test_data(test_size);
        std::mt19937 gen(42);
        std::uniform_int_distribution<> dis(0, 255);

        for (uint64_t i = 0; i < test_size; ++i) {
            // Mezclamos aleatoriedad con repetición para simular datos reales
            if (i % 4 == 0)
                test_data[i] = (char)dis(gen);
            else
                test_data[i] = (char)(i % 10);
        }

        std::vector<char> comp_buffer(LZ4_compressBound(test_size));
        const int iterations = 100;  // Suficientes para calentar la caché y promediar

        auto start = std::chrono::high_resolution_clock::now();

        int compress_size = 0;
        for (int i = 0; i < iterations; ++i) {
            compress_size = LZ4_compress_fast(test_data.data(), comp_buffer.data(), test_size, comp_buffer.size(), 20);
        }
        for (int i = 0; i < iterations; ++i) {
            LZ4_decompress_safe(comp_buffer.data(), test_data.data(), compress_size, test_size);
        }

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> elapsed = end - start;

        // MB/s que la CPU puede comprimir
        calibrated_result = (static_cast<double>(test_size) * iterations / (1024.0 * 1024.0)) / elapsed.count();

        debug_info("--- CALIBRATION ---");
        debug_info("CPU velocity (LZ4): " << calibrated_result << " MB/s");
        debug_info("Ellapsed time: " << elapsed.count() << " secs");
        debug_info("Compressed from: " << test_size << " to " << compress_size);
        debug_info("-------------------");
        return calibrated_result;
    }

    // Función principal de decisión
    Result process_write(const char* input, uint64_t inputSize, std::unique_lock<std::mutex>& buffer_lock, std::vector<char>& compressBuffer) {

        if (should_compress(inputSize, m_currently_compressing_write, m_mbps_avg_write)) {
            int maxCompressedSize = LZ4_compressBound(inputSize);
            if (!buffer_lock.owns_lock()) buffer_lock.lock();
            if (compressBuffer.size() < static_cast<size_t>(maxCompressedSize)){
                compressBuffer.resize(maxCompressedSize);
            }

            int compressedSize = LZ4_compress_fast(input, compressBuffer.data(), inputSize, maxCompressedSize, 10);

            debug_info("Compressed form " << inputSize << " to " << compressedSize << " because avg "
                                          << m_mbps_avg_write << " is less than limit " << m_cpu_limit_mbps);
            if (compressedSize > 0 && compressedSize < (inputSize * 0.95)) {
                return {compressBuffer.data(), (size_t)compressedSize, true};
            }
        }

        return {input, inputSize, false};
    }

    bool should_compress_read(size_t size) {
        return should_compress(size, m_currently_compressing_read, m_mbps_avg_read);
    }

    void update_metrics_write(uint64_t bytes, std::chrono::nanoseconds totalDuration,
                              std::chrono::nanoseconds serverProcessingTime) {
        auto netDuration = totalDuration - serverProcessingTime;
        double seconds = netDuration.count() / 1e9;
        double current_mbps = (bytes / (1024.0 * 1024.0)) / seconds;
        m_mbps_avg_write = (current_mbps * m_alpha) + (m_mbps_avg_write * (1.0 - m_alpha));
        debug_info(std::chrono::duration_cast<std::chrono::microseconds>(serverProcessingTime)
                   << "/" << std::chrono::duration_cast<std::chrono::microseconds>(totalDuration)
                   << " server/total Net Mbps: " << current_mbps << " (Avg: " << m_mbps_avg_write << ")");
    }

    void update_metrics_read(uint64_t bytes, std::chrono::nanoseconds totalDuration,
                             std::chrono::nanoseconds serverProcessingTime) {
        auto netDuration = totalDuration - serverProcessingTime;
        double seconds = netDuration.count() / 1e9;
        double current_mbps = (bytes / (1024.0 * 1024.0)) / seconds;
        m_mbps_avg_read = (current_mbps * m_alpha) + (m_mbps_avg_read * (1.0 - m_alpha));
        
        debug_info(std::chrono::duration_cast<std::chrono::microseconds>(serverProcessingTime)
                   << "/" << std::chrono::duration_cast<std::chrono::microseconds>(totalDuration)
                   << " server/total Net Mbps: " << current_mbps << " (Avg: " << m_mbps_avg_read << ")");
    }

   private:
    bool should_compress(uint64_t size, bool& currently_compressing, double mbps_avg) {
        if (xpn_env::get_instance().xpn_compression == 0) return false;
        if (xpn_env::get_instance().xpn_compression == 2) return true;
        if (size < ignore_compress_size) return false;
        if (m_cpu_limit_mbps < 0) m_cpu_limit_mbps = calibrate(MAX_BUFFER_SIZE);
        double activation_threshold = m_cpu_limit_mbps;
        double deactivation_threshold = m_cpu_limit_mbps * 1.3; // 30% fo margin

        if (!currently_compressing) {
            if (mbps_avg < activation_threshold) {
                currently_compressing = true;
            }
        } else {
            if (mbps_avg > deactivation_threshold) {
                currently_compressing = false;
            }
        }

        return currently_compressing;
    }
};
}  // namespace XPN