
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
template <size_t Capacity>
class FixedString {
   private:
    char m_data[Capacity + 1];
    size_t m_size = 0;

   public:
    FixedString() { clear(); }

    FixedString(const char* str) { assign(str); }

    void clear() {
        m_size = 0;
        m_data[0] = '\0';
    }

    void assign(const char* str) {
        clear();
        append(str);
    }

    void append(const char* str) {
        if (!str) return;
        append(str, std::strlen(str));
    }

    void append(const std::string& str) { append(str.data(), str.size()); }
    void append(const std::string_view& str) { append(str.data(), str.size()); }

    void append(const char* src, size_t len) {
        const size_t space_left = Capacity - m_size;
        const size_t to_copy = std::min(len, space_left);

        if (to_copy > 0) {
            std::memcpy(m_data + m_size, src, to_copy);
            m_size += to_copy;
            m_data[m_size] = '\0';
        }
    }

    FixedString& operator+=(const char* str) {
        append(str);
        return *this;
    }

    size_t copy(char* dest, size_t len, size_t pos = 0) const {
        if (pos > m_size) {
            throw std::out_of_range("FixedString::copy: pos > size()");
        }
        const size_t count = std::min(len, m_size - pos);
        if (count > 0) {
            std::memcpy(dest, m_data + pos, count);
        }
        return count;
    }

    const char* c_str() const { return m_data; }
    size_t size() const { return m_size; }
    size_t capacity() const { return Capacity; }
    bool empty() const { return m_size == 0; }

    char& operator[](size_t index) { return m_data[index]; }
    const char& operator[](size_t index) const { return m_data[index]; }

    operator std::string_view() const { return std::string_view(m_data, m_size); }

    friend std::ostream& operator<<(std::ostream& os, const FixedString<Capacity>& fs) {
        os << std::string_view(fs.m_data, fs.m_size);
        return os;
    }
};

using FixedStringPath = FixedString<PATH_MAX>;
}  // namespace XPN