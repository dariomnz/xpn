
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
#include <future>
#include <list>
#include <mutex>
#include <sstream>
#include <string>
#include <variant>
#include <vector>
#include "base_cpp/task_result.hpp"

namespace XPN {

struct profiler_data {
    pid_t m_pid;
    std::thread::id m_tid;
    std::variant<const char*, std::string> m_name;
    uint32_t m_start;
    uint32_t m_duration;

    profiler_data(pid_t pid, std::thread::id tid, std::variant<const char*, std::string> name, uint32_t start,
                  uint32_t duration)
        : m_pid(pid), m_tid(tid), m_name(name), m_start(start), m_duration(duration) {}

    void dump_data(std::ostream& json, std::string_view process_name) const;
};

class profiler {
   public:
    profiler(const profiler&) = delete;
    profiler(profiler&&) = delete;

    void begin_session(std::string_view name);
    void end_session();

    void write_profile(std::variant<const char*, std::string> name, uint32_t start, uint32_t duration);

    static std::string get_header();
    static std::string get_footer();

    static profiler& get_instance() {
        static profiler instance;
        return instance;
    }

   private:
    profiler();

    ~profiler();

    void save_data(const std::vector<profiler_data>&& message);

   private:
    std::mutex m_mutex;
    std::mutex m_mutex_fut_save_data;
    std::string m_current_session = "";
    std::string m_hostname = "";
    std::list<TaskResult<int>> m_fut_save_data;
    constexpr static uint64_t m_buffer_cap = 1024;
    std::vector<profiler_data> m_buffer;
};

class profiler_timer {
   public:
    profiler_timer(const char* name) : m_name(name) { m_start_timepoint = std::chrono::high_resolution_clock::now(); }

    template <typename... Args>
    profiler_timer(Args... args) {
        m_start_timepoint = std::chrono::high_resolution_clock::now();
        m_name_str = concatenate(args...);
    }

    template <typename T>
    std::string toString(const T& value) {
        std::stringstream ss;
        ss << value;
        return ss.str();
    }

    template <typename T, typename... Rest>
    std::string concatenate(const T& value, const Rest&... rest) {
        std::string result = toString(value);
        if constexpr (sizeof...(rest) > 0) {
            result += ", " + concatenate(rest...);
        }
        return result;
    }

    ~profiler_timer() {
        if (!m_stopped) Stop();
    }

    void Stop() {
        auto start =
            std::chrono::duration_cast<std::chrono::microseconds>(m_start_timepoint.time_since_epoch()).count();
        auto elapsed_time = std::chrono::duration_cast<std::chrono::microseconds>(
                                std::chrono::high_resolution_clock::now() - m_start_timepoint)
                                .count();
        if (m_name == nullptr) {
            profiler::get_instance().write_profile(m_name_str, start, elapsed_time);
        } else {
            profiler::get_instance().write_profile(m_name, start, elapsed_time);
        }
        m_stopped = true;
    }

   private:
    const char* m_name = nullptr;
    std::string m_name_str;
    std::chrono::time_point<std::chrono::high_resolution_clock> m_start_timepoint;
    bool m_stopped = false;
};
}  // namespace XPN

#define XPN_PROFILE 1
#if XPN_PROFILE
#define XPN_PROFILE_BEGIN_SESSION(name) ::XPN::profiler::get_instance().begin_session(name)
#define XPN_PROFILE_END_SESSION() ::XPN::profiler::get_instance().end_session()
#define XPN_PROFILE_SCOPE_LINE2(name, line)     ::XPN::profiler_timer timer##line(name)
#define XPN_PROFILE_SCOPE_LINE(name, line)      XPN_PROFILE_SCOPE_LINE2(name, line)
#define XPN_PROFILE_SCOPE(name)                 XPN_PROFILE_SCOPE_LINE(name, __LINE__)
#define XPN_PROFILE_FUNCTION()                  XPN_PROFILE_SCOPE(__func__)

#define XPN_PROFILE_SCOPE_ARGS_LINE2(line, ...) ::XPN::profiler_timer timer##line(__VA_ARGS__)
#define XPN_PROFILE_SCOPE_ARGS_LINE(line, ...)  XPN_PROFILE_SCOPE_ARGS_LINE2(line, __VA_ARGS__)
#define XPN_PROFILE_SCOPE_ARGS(name, ...)       XPN_PROFILE_SCOPE_ARGS_LINE(__LINE__, name, __VA_ARGS__)
#define XPN_PROFILE_FUNCTION_ARGS(...)          XPN_PROFILE_SCOPE_ARGS(__func__, __VA_ARGS__)
#else
#define XPN_PROFILE_BEGIN_SESSION(name)
#define XPN_PROFILE_END_SESSION()
#define XPN_PROFILE_SCOPE(name)
#define XPN_PROFILE_FUNCTION()

#define XPN_PROFILE_SCOPE_ARGS(name, ...)
#define XPN_PROFILE_FUNCTION_ARGS(...)
#endif