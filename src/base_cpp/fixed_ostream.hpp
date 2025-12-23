
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

#include <linux/limits.h>

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <ostream>

namespace XPN {
struct FixedBuffer : std::streambuf {
    FixedBuffer(char* buf, size_t size) { setp(buf, buf + size - 1); }

    size_t size() const { return pptr() - pbase(); }

    const char* c_str() {
        *pptr() = '\0';
        return pbase();
    }
};

template <size_t Capacity>
struct FixedOStreamStorage {
    char m_buffer[Capacity];
    FixedBuffer m_storage;

    FixedOStreamStorage() : m_storage(m_buffer, Capacity) {}
};

template <size_t Capacity>
struct FixedOStream : private FixedOStreamStorage<Capacity>, public std::ostream {
    FixedOStream() : FixedOStreamStorage<Capacity>(), std::ostream(&this->m_storage) {}

    std::string_view sv() const {
        const auto storage = this->m_storage;
        return std::string_view(storage.c_str(), storage.size());
    }

    size_t size() const { return this->m_storage.size(); }
    const char* c_str() { return this->m_storage.c_str(); }
};
}  // namespace XPN