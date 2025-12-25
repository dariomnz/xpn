
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

#include <cstddef>
#include <memory_resource>
#include <string>

namespace XPN {

template <size_t Capacity>
struct GrowFixedStringStorage {
    char m_buffer[Capacity + 1];
    std::pmr::monotonic_buffer_resource m_pool;

    // Default contructor
    GrowFixedStringStorage() : m_pool(m_buffer, sizeof(m_buffer)) {}
    // Copy contructor
    GrowFixedStringStorage(const GrowFixedStringStorage&) : GrowFixedStringStorage() {}
    // Move contructor
    GrowFixedStringStorage(GrowFixedStringStorage&&) noexcept : GrowFixedStringStorage() {}
};

template <size_t Capacity>
class GrowFixedString : private GrowFixedStringStorage<Capacity>, public std::pmr::string {
   public:
    GrowFixedString() : GrowFixedStringStorage<Capacity>(), std::pmr::string(&(this->m_pool)) { this->reserve(Capacity); }

    GrowFixedString(const char* s) : GrowFixedStringStorage<Capacity>(), std::pmr::string(&(this->m_pool)) {
        this->reserve(Capacity);
        this->append(s);
    }

    GrowFixedString(std::string_view sv) : GrowFixedStringStorage<Capacity>(), std::pmr::string(&(this->m_pool)) {
        this->reserve(Capacity);
        this->append(sv);
    }

    // Copy contructor
    GrowFixedString(const GrowFixedString& other)
        : GrowFixedStringStorage<Capacity>(), std::pmr::string(other, &(this->m_pool)) {
        this->reserve(Capacity);
    }
    // Move constructor
    GrowFixedString(GrowFixedString&& other) noexcept
        : GrowFixedStringStorage<Capacity>(), std::pmr::string(std::move(other), &(this->m_pool)) {
        this->reserve(Capacity);
    }
    // Copy assigment
    GrowFixedString& operator=(const GrowFixedString& other) {
        if (this != &other) {
            std::pmr::string::operator=(other);
        }
        return *this;
    }
    // Move assigment
    GrowFixedString& operator=(GrowFixedString&& other) noexcept {
        if (this != &other) {
            std::pmr::string::operator=(std::move(other));
        }
        return *this;
    }
};
}  // namespace XPN