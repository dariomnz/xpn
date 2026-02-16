
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

#include "ns.hpp"
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
    json << "\"tid\":\"" << process_name << "_" << m_tid << "\",";
    json << "\"ts\":" << m_start;
    json << "}\n";
}

void profiler::begin_session(std::string_view name) {
    debug_info("begin profiler session " << name);
    std::unique_lock lock(m_mutex);
    m_current_session = name;
    m_hostname = ns::get_host_name();
    m_buffer.reserve(m_buffer_cap);
}

void profiler::end_session() {
    debug_info("end profiler session " << m_current_session);
    {
        std::unique_lock lock(m_mutex);
        if (m_buffer.size() > 0) {
            save_data(std::move(m_buffer));
            m_buffer.clear();
        }
        m_current_session.clear();
    }
    {
        std::unique_lock lock(m_mutex_fut_save_data);
        for (auto& fut : m_fut_save_data) {
            if (fut.valid()) {
                fut.get();
            }
        }
    }
}

void profiler::write_profile(std::variant<const char*, std::string> name, uint32_t start, uint32_t duration) {
    std::unique_lock lock(m_mutex);
    if (!m_current_session.empty()) {
#ifdef DEBUG
        if (std::holds_alternative<const char*>(name)) {
            debug_info("Write profile: '" << std::get<const char*>(name) << "' Buffer size " << m_buffer.size());
        } else {
            debug_info("Write profile: '" << std::get<std::string>(name) << "' Buffer size " << m_buffer.size());
        }
#endif
        m_buffer.emplace_back(getpid(), std::this_thread::get_id(), name, start, duration);

        if (m_buffer.size() >= m_buffer_cap) {
            save_data(std::move(m_buffer));
            m_buffer.clear();
            m_buffer.reserve(m_buffer_cap);
        }
    }
}

profiler::profiler() {
    std::unique_lock lock(m_mutex);
    m_buffer.clear();
    m_buffer.reserve(m_buffer_cap);
}

profiler::~profiler() { end_session(); }

std::string profiler::get_header() { return "{\"otherData\": {},\"traceEvents\":[{}\n"; }

std::string profiler::get_footer() { return "]}"; }

void profiler::save_data(const std::vector<profiler_data>&& message) {
    TaskResult<int>* task;
    {
        std::unique_lock lock(m_mutex_fut_save_data);
        m_fut_save_data.remove_if([](auto& fut) {
            if (fut.ready()) {
                fut.get();
                return true;
            }
            return false;
        });
        task = &m_fut_save_data.emplace_back();
        task->init();
    }
    auto current_session = m_current_session + "_" + m_hostname;
    std::thread([msg = std::move(message), current_session, task]() {
        std::stringstream ss;
        for (auto& data : msg) {
            data.dump_data(ss, current_session);
        }
        std::string str = ss.str();
        debug_info("profiler send msgs " << msg.size() << " and " << str.size() << " str size");
        task->set_value(xpn_controller::send_profiler(str));
    }).detach();
}
}  // namespace XPN