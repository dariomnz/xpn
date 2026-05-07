
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

#include <chrono>
#include <mutex>

namespace XPN {
class StatTraker {
   private:
    const double alpha = 0.1;
    std::chrono::milliseconds window_time{100};

    uint64_t totalBytesInWindow = 0;
    uint64_t sampleCount = 0;
    std::chrono::high_resolution_clock::time_point windowStartTime;

    double emaMbps = 0.0;
    bool initialized = false;

    std::mutex m_mutex;

   public:
    explicit StatTraker() : windowStartTime(std::chrono::high_resolution_clock::now()) {}

    void add_sample(uint64_t bytes) {
        auto now = std::chrono::high_resolution_clock::now();

        std::unique_lock lock(m_mutex);
        auto elapsed = now - windowStartTime;

        if (elapsed >= window_time) {
            double windowMbps = (static_cast<double>(totalBytesInWindow)) / (1024.0 * 1024.0) /
                                (std::chrono::duration_cast<std::chrono::microseconds>(window_time).count() / 1e6);
            if (windowMbps > 0.01) {
                if (!initialized) {
                    emaMbps = windowMbps;
                    initialized = true;
                } else {
                    emaMbps = (windowMbps * alpha) + (emaMbps * (1.0 - alpha));
                }
            }

            totalBytesInWindow = 0;
            sampleCount = 0;
            windowStartTime = now;
        }

        totalBytesInWindow += bytes;
        sampleCount++;
    }

    double get_mbps() const {
        if (!initialized) {
            auto now = std::chrono::high_resolution_clock::now();
            double windowMbps =
                (static_cast<double>(totalBytesInWindow)) / (1024.0 * 1024.0) /
                (std::chrono::duration_cast<std::chrono::microseconds>(now - windowStartTime).count() / 1e6);
            return windowMbps;
        }

        return emaMbps;
    }
};
}  // namespace XPN
