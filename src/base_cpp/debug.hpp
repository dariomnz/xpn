
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
#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>

#include "fixed_ostream.hpp"
#include "profiler.hpp"
#include "xpn_env.hpp"

namespace XPN {
constexpr const char *file_name(const char *path) {
    const char *file = path;
    while (*path) {
        if (*path++ == '/') {
            file = path;
        }
    }
    return file;
}

struct get_time_stamp {
    friend std::ostream &operator<<(std::ostream &os, const get_time_stamp &time_stamp);
};

struct format_open_flags {
    int m_flags;
    format_open_flags(int flags) : m_flags(flags) {}
    friend std::ostream &operator<<(std::ostream &os, const format_open_flags &open_flags);
};

struct format_open_mode {
    mode_t m_mode;
    format_open_mode(mode_t mode) : m_mode(mode) {}
    friend std::ostream &operator<<(std::ostream &os, const format_open_mode &open_mode);
};
struct format_bytes {
    uint64_t bytes;
    int precision;
    explicit format_bytes(uint64_t b, int p = 2) : bytes(b), precision(p) {}
    friend std::ostream &operator<<(std::ostream &os, const format_bytes &fb);
};

struct print_errno {
    ssize_t res;
    print_errno(ssize_t res) : res(res) {}
    friend std::ostream &operator<<(std::ostream &os, const print_errno &pe) {
        if (pe.res < 0) {
            os << " errno=" << errno << " " << std::strerror(errno);
        }
        return os;
    }
};

struct static_debug_mutex {
    static std::recursive_mutex &get() {
        static std::recursive_mutex static_mutex;
        return static_mutex;
    }
};

static constexpr int DEBUG_BUFFER_SIZE = 4096;

#define XPN_DEBUG_COMMON_HEADER                                                                                    \
    {                                                                                                              \
        ::XPN::FixedCerrStream<::XPN::DEBUG_BUFFER_SIZE> out;                                                      \
        out << "[" << ::XPN::get_time_stamp() << "] [" << std::this_thread::get_id() << "] [" << __func__ << "] [" \
            << ::XPN::file_name(__FILE__) << ":" << __LINE__ << "] ";                                              \
    }

#ifdef DEBUG
#define XPN_DEBUG(out_format)                                                      \
    {                                                                              \
        std::unique_lock __xpn_debug_print_lock(::XPN::static_debug_mutex::get()); \
        XPN_DEBUG_COMMON_HEADER                                                    \
        ::XPN::FixedCerrStream<::XPN::DEBUG_BUFFER_SIZE> out;                      \
        out << out_format << std::endl;                                            \
    }
#else
#define XPN_DEBUG(out_format)                                                      \
    if (::XPN::xpn_env::get_instance().xpn_debug) {                                \
        std::unique_lock __xpn_debug_print_lock(::XPN::static_debug_mutex::get()); \
        XPN_DEBUG_COMMON_HEADER                                                    \
        ::XPN::FixedCerrStream<::XPN::DEBUG_BUFFER_SIZE> out;                      \
        out << out_format << std::endl;                                            \
    }
#endif

#define XPN_DEBUG_BEGIN_CUSTOM(out_format)                       \
    XPN_DEBUG("Begin " << __func__ << "(" << out_format << ")"); \
    XPN_PROFILE_FUNCTION();
#define XPN_DEBUG_END_CUSTOM(out_format) \
    XPN_DEBUG("End   " << __func__ << "(" << out_format << ")=" << res << print_errno(res));
#define XPN_DEBUG_BEGIN                      \
    XPN_DEBUG("Begin " << __func__ << "()"); \
    XPN_PROFILE_FUNCTION();
#define XPN_DEBUG_END XPN_DEBUG("End   " << __func__ << "()=" << res << print_errno(res));

#ifdef DEBUG
#undef debug_error
#define debug_error(out_format)                                                                                   \
    {                                                                                                             \
        std::unique_lock __xpn_debug_print_lock(::XPN::static_debug_mutex::get());                                \
        ::XPN::FixedCerrStream<::XPN::DEBUG_BUFFER_SIZE> out;                                                     \
        out << "[ERROR] [" << ::XPN::get_time_stamp() << "] [" << std::this_thread::get_id() << "] [" << __func__ \
            << "] [" << ::XPN::file_name(__FILE__) << ":" << __LINE__ << "] " << out_format << std::endl;         \
    }
#undef debug_warning
#define debug_warning(out_format)                                                                                   \
    {                                                                                                               \
        std::unique_lock __xpn_debug_print_lock(::XPN::static_debug_mutex::get());                                  \
        ::XPN::FixedCerrStream<::XPN::DEBUG_BUFFER_SIZE> out;                                                       \
        out << "[WARNING] [" << ::XPN::get_time_stamp() << "] [" << std::this_thread::get_id() << "] [" << __func__ \
            << "] [" << ::XPN::file_name(__FILE__) << ":" << __LINE__ << "] " << out_format << std::endl;           \
    }
#undef debug_info
#define debug_info(out_format)                                                                                   \
    {                                                                                                            \
        std::unique_lock __xpn_debug_print_lock(::XPN::static_debug_mutex::get());                               \
        ::XPN::FixedCerrStream<::XPN::DEBUG_BUFFER_SIZE> out;                                                    \
        out << "[INFO] [" << ::XPN::get_time_stamp() << "] [" << std::this_thread::get_id() << "] [" << __func__ \
            << "] [" << ::XPN::file_name(__FILE__) << ":" << __LINE__ << "] " << out_format << std::endl;        \
    }
#undef debug_info_fmt
#define debug_info_fmt(...)                  \
    {                                        \
        char buff[::XPN::DEBUG_BUFFER_SIZE]; \
        ::sprintf(buff, __VA_ARGS__);        \
        debug_info(buff);                    \
    }
#else
#define debug_error(out_format)
#define debug_warning(out_format)
#define debug_info(out_format)
#define debug_info_fmt(...)
#endif

#undef print
#define print(out_format)                                       \
    do {                                                        \
        ::XPN::FixedCoutStream<::XPN::DEBUG_BUFFER_SIZE> __out; \
        __out << out_format << std::endl;                       \
    } while (0);
#undef print_fmt
#define print_fmt(...)                       \
    do {                                     \
        char buff[::XPN::DEBUG_BUFFER_SIZE]; \
        ::sprintf(buff, __VA_ARGS__);        \
        print(buff);                         \
    } while (0);

#undef print_error
#define print_error(out_format)                                                                                        \
    std::cerr << std::dec << "[ERROR] [" << ::XPN::file_name(__FILE__) << ":" << __LINE__ << "] [" << __func__ << "] " \
              << out_format << " : " << std::strerror(errno) << std::endl;

#define unreachable(msg) \
    print_error(msg);    \
    std::abort();
}  // namespace XPN