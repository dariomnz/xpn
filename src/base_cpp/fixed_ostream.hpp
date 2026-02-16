
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
    FILE* m_file;

    FixedBuffer(char* buf, size_t size, FILE* file) : m_file(file) { setp(buf, buf + size); }

    ~FixedBuffer() override { sync(); }

    int_type overflow(int_type c) override {
        if (m_file) {
            flush_to_file();
            if (!traits_type::eq_int_type(c, traits_type::eof())) {
                sputc(traits_type::to_char_type(c));
            }
            return c;
        }
        return traits_type::eof();
    }

    int sync() override {
        if (m_file) {
            flush_to_file();
        }
        return 0;
    }

    void flush_to_file() {
        if (!m_file) return;
        std::ptrdiff_t n = pptr() - pbase();
        if (n > 0) {
            fprintf(m_file, "%.*s", static_cast<int>(n), pbase());
            fflush(m_file);
            setp(pbase(), epptr());  // Reset pointers
        }
    }
};

template <size_t Capacity>
struct FixedOStreamStorage {
    char m_buffer[Capacity];
    FixedBuffer m_storage;

    FixedOStreamStorage(FILE* file) : m_storage(m_buffer, Capacity, file) {}
};

template <size_t Capacity>
struct FixedOStream : private FixedOStreamStorage<Capacity>, public std::ostream {
    FixedOStream() : FixedOStreamStorage<Capacity>(nullptr), std::ostream(&this->m_storage) {}
    FixedOStream(FILE* file) : FixedOStreamStorage<Capacity>(file), std::ostream(&this->m_storage) {}

    void clear_buffer() {
        this->m_storage.clear();
        this->clear();
    }

    std::string_view sv() const { return std::string_view(this->m_storage.pbase(), this->m_storage.size()); }
};

template <size_t Capacity>
struct FixedCoutStream : public FixedOStream<Capacity> {
    FixedCoutStream() : FixedOStream<Capacity>(stdout) {}
};

template <size_t Capacity>
struct FixedCerrStream : public FixedOStream<Capacity> {
    FixedCerrStream() : FixedOStream<Capacity>(stderr) {}
};
}  // namespace XPN