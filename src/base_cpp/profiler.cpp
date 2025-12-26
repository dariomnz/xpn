
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

#include "profiler.hpp"

#include <errno.h>
#include <string.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>

#include "filesystem.hpp"
#include "ns.hpp"
#include "socket.hpp"
#include "xpn_controller/xpn_controller.hpp"

namespace XPN {

void profiler_data::dump_data(std::ostream& json, std::string_view process_name) const {
    json << ",{";
    json << "\"cat\":\"function\",";
    json << "\"dur\":" << m_duration << ',';
    if (std::holds_alternative<const char*>(m_name)) {
        json << "\"name\":\"" << std::get<const char*>(m_name) << "\",";
    } else {
        json << "\"name\":\"" << std::get<std::string>(m_name) << "\",";
    }
    json << "\"ph\":\"X\",";
    json << "\"pid\":\"" << process_name << "_" << m_pid << "\",";
    json << "\"tid\":" << m_tid << ",";
    json << "\"ts\":" << m_start;
    json << "}\n";
}

void profiler::begin_session(std::string_view name) {
    std::lock_guard lock(m_mutex);
    m_current_session = name;
    m_hostname = ns::get_host_name();
}

void profiler::write_profile(std::variant<const char*, std::string> name, uint32_t start, uint32_t duration) {
    std::lock_guard lock(m_mutex);
    if (!m_current_session.empty()) {
        debug_info("Buffer size " << m_buffer.size());
        m_buffer.emplace_back(getpid(), std::this_thread::get_id(), name, start, duration);

        if (m_buffer.size() >= m_buffer_cap) {
            save_data(std::move(m_buffer));
            m_buffer = std::vector<profiler_data>();
            m_buffer.reserve(m_buffer_cap);
        }
    }
}

profiler::profiler() {
    std::lock_guard lock(m_mutex);
    m_buffer = std::vector<profiler_data>();
    m_buffer.reserve(m_buffer_cap);
}

profiler::~profiler() {
    std::lock_guard lock(m_mutex);
    if (m_buffer.size() > 0) {
        save_data(std::move(m_buffer));
    }
    for (auto& fut : m_fut_save_data) {
        if (fut.valid()) {
            fut.get();
        }
    }
}

std::string profiler::get_header() { return "{\"otherData\": {},\"traceEvents\":[{}\n"; }

std::string profiler::get_footer() { return "]}"; }

void profiler::save_data(const std::vector<profiler_data>&& message) {
    m_fut_save_data.remove_if([](auto& fut) {
        if (fut.valid() && fut.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
            fut.get();
            return true;
        }
        return false;
    });
    auto current_session = m_current_session + "_" + m_hostname;
    m_fut_save_data.push_back(std::async(std::launch::async, [msg = std::move(message), current_session]() {
        std::stringstream ss;
        for (auto& data : msg) {
            data.dump_data(ss, current_session);
        }
        std::string str = ss.str();
        debug_info("profiler send msgs " << msg.size() << " and " << str.size() << " str size");
        return xpn_controller::send_profiler(str);
    }));
}
}  // namespace XPN