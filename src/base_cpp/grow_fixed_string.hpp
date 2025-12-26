
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

#include <string>

#include "grow_fixed_allocator.hpp"

namespace XPN {

template <size_t Capacity>
struct GrowFixedStringStorage {
    InternalArena<Capacity + 1> m_arena;
    GrowFixedAllocator<char, Capacity + 1> m_allocator;

    GrowFixedStringStorage() : m_arena(), m_allocator(m_arena) {}
    // Copy contructor
    GrowFixedStringStorage(const GrowFixedStringStorage &) : GrowFixedStringStorage() {}
    // Move contructor
    GrowFixedStringStorage(GrowFixedStringStorage &&) noexcept : GrowFixedStringStorage() {}
};

template <size_t Capacity>
class GrowFixedString : public GrowFixedStringStorage<Capacity>,
                       public std::basic_string<char, std::char_traits<char>, GrowFixedAllocator<char, Capacity + 1>> {
    using AllocType = GrowFixedAllocator<char, Capacity + 1>;
    using StringType = std::basic_string<char, std::char_traits<char>, AllocType>;

   public:
    GrowFixedString() : GrowFixedStringStorage<Capacity>(), StringType(AllocType(this->m_allocator)) {
        this->reserve(Capacity);
    }

    GrowFixedString(const char *text) : GrowFixedStringStorage<Capacity>(), StringType(AllocType(this->m_allocator)) {
        this->reserve(Capacity);
        this->append(text);
    }

    GrowFixedString(const std::string &text)
        : GrowFixedStringStorage<Capacity>(), StringType(AllocType(this->m_allocator)) {
        this->reserve(Capacity);
        this->append(text);
    }

    GrowFixedString(std::string_view text)
        : GrowFixedStringStorage<Capacity>(), StringType(AllocType(this->m_allocator)) {
        this->reserve(Capacity);
        this->append(text);
    }

    // Copy contructor
    GrowFixedString(const GrowFixedString &other)
        : GrowFixedStringStorage<Capacity>(), StringType(AllocType(this->m_allocator)) {
        this->reserve(Capacity);
        this->append(other);
    }
    // Move constructor
    GrowFixedString(GrowFixedString &&other) noexcept
        : GrowFixedStringStorage<Capacity>(), StringType(AllocType(this->m_allocator)) {
        this->reserve(Capacity);
        this->append(other);
    }
    // Copy assigment
    GrowFixedString &operator=(const GrowFixedString &other) {
        if (this != &other) {
            StringType::operator=(other);
        }
        return *this;
    }
    // Move assigment
    GrowFixedString &operator=(GrowFixedString &&other) noexcept {
        if (this != &other) {
            StringType::operator=(std::move(other));
        }
        return *this;
    }
};
}  // namespace XPN