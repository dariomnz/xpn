
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

/* ... Include / Inclusion ........................................... */
#define DEBUG
#include "base_cpp/debug.hpp"
#include "base_cpp/proxy.hpp"

#undef print
#undef print_fmt

#define print(out_format)                                                          \
    do {                                                                           \
        std::unique_lock __xpn_debug_print_lock(::XPN::static_debug_mutex::get()); \
        ::XPN::FixedCerrStream<::XPN::DEBUG_BUFFER_SIZE> __out;                    \
        __out << out_format << std::endl;                                          \
    } while (0);
#define print_fmt(...)                       \
    do {                                     \
        char buff[::XPN::DEBUG_BUFFER_SIZE]; \
        ::sprintf(buff, __VA_ARGS__);        \
        print(buff);                         \
    } while (0);

static inline void set_buffs() {
    static bool flag = false;
    if (!flag) {
        flag = true;
        setbuf(stdout, nullptr);
        setbuf(stderr, nullptr);
    }
}

extern "C" void *malloc(size_t size) {
    set_buffs();
    void *ret;
    print_fmt("[BYPASS] >> Begin malloc(%ld)", size);
    ret = PROXY(malloc)(size);
    print_fmt("[BYPASS] << PROXY(malloc)(%ld) -> %p", size, ret);
    return ret;
}

extern "C" void *realloc(void *old_ptr, size_t size) {
    set_buffs();
    void *ret;
    print_fmt("[BYPASS] >> Begin realloc(%p, %ld)", old_ptr, size);
    ret = PROXY(realloc)(old_ptr, size);
    print_fmt("[BYPASS] << PROXY(realloc)(%p, %ld) -> %p", old_ptr, size, ret);
    return ret;
}

extern "C" void free(void *ptr) {
    set_buffs();
    print_fmt("[BYPASS] >> Begin free(%p)", ptr);
    PROXY(free)(ptr);
    print_fmt("[BYPASS] << PROXY(free)(%p)", ptr);
}