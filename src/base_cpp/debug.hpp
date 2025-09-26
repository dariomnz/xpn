
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
    friend std::ostream &operator<<(std::ostream &os, [[maybe_unused]] const get_time_stamp &time_stamp);
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

struct print_errno {
    ssize_t res;
    print_errno(ssize_t res) : res(res) {}
    friend std::ostream &operator<<(std::ostream &os, [[maybe_unused]] const print_errno &pe) {
        if (pe.res < 0) {
            os << " errno=" << errno << " " << std::strerror(errno);
        }
        return os;
    }
};

#define XPN_DEBUG_COMMON_HEADER                                                                                \
    {                                                                                                          \
        std::ostringstream out;                                                                                \
        out << "[" << get_time_stamp() << "] [" << __func__ << "] [" << file_name(__FILE__) << ":" << __LINE__ \
            << "] ";                                                                                           \
        fprintf(stderr, out.str().c_str());                                                                    \
    }

#ifdef DEBUG
#define XPN_DEBUG(out_format)               \
    {                                       \
        XPN_DEBUG_COMMON_HEADER             \
        std::ostringstream out;             \
        out << out_format << std::endl;     \
        fprintf(stderr, out.str().c_str()); \
    }
#else
#define XPN_DEBUG(out_format)                \
    if (xpn_env::get_instance().xpn_debug) { \
        XPN_DEBUG_COMMON_HEADER              \
        std::ostringstream out;              \
        out << out_format << std::endl;      \
        fprintf(stderr, out.str().c_str());  \
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
#define debug_error(out_format)                                                                                        \
    {                                                                                                                  \
        std::ostringstream out;                                                                                        \
        out << "[ERROR] [" << __func__ << "] [" << ::XPN::file_name(__FILE__) << ":" << __LINE__ << "] " << out_format \
            << std::endl;                                                                                              \
        fprintf(stderr, out.str().c_str());                                                                            \
    }
#define debug_warning(out_format)                                                                          \
    {                                                                                                      \
        std::ostringstream out;                                                                            \
        out << "[WARNING] [" << __func__ << "] [" << ::XPN::file_name(__FILE__) << ":" << __LINE__ << "] " \
            << out_format << std::endl;                                                                    \
        fprintf(stderr, out.str().c_str());                                                                \
    }
#define debug_info(out_format)                                                                                        \
    {                                                                                                                 \
        std::ostringstream out;                                                                                       \
        out << "[INFO] [" << __func__ << "] [" << ::XPN::file_name(__FILE__) << ":" << __LINE__ << "] " << out_format \
            << std::endl;                                                                                             \
        fprintf(stderr, out.str().c_str());                                                                           \
    }
#else
#define debug_error(out_format)
#define debug_warning(out_format)
#define debug_info(out_format)
#endif

#define print(out_format) std::cout << out_format << std::endl;

#define print_error(out_format)                                                                                        \
    std::cerr << std::dec << "[ERROR] [" << ::XPN::file_name(__FILE__) << ":" << __LINE__ << "] [" << __func__ << "] " \
              << out_format << " : " << std::strerror(errno) << std::endl;

#define unreachable(msg) \
    print_error(msg);    \
    std::abort();
}  // namespace XPN