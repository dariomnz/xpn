
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

template <typename T, size_t Capacity>
struct GrowFixedVectorStorage {
    char m_buffer[sizeof(T) * Capacity];
    std::pmr::monotonic_buffer_resource m_pool;

    // Default contructor
    GrowFixedVectorStorage() : m_pool(m_buffer, sizeof(m_buffer)) {}
    // Copy contructor
    GrowFixedVectorStorage(const GrowFixedVectorStorage&) : GrowFixedVectorStorage() {}
    // Move contructor
    GrowFixedVectorStorage(GrowFixedVectorStorage&&) noexcept : GrowFixedVectorStorage() {}
};

template <typename T, size_t Capacity>
class GrowFixedVector : private GrowFixedVectorStorage<T, Capacity>, public std::pmr::vector<T> {
   public:
    // Default contructor
    GrowFixedVector() : GrowFixedVectorStorage<T, Capacity>(), std::pmr::vector<T>(&(this->m_pool)) {
        this->reserve(Capacity);
    }

    // Copy contructor
    GrowFixedVector(const GrowFixedVector& other)
        : GrowFixedVectorStorage<T, Capacity>(), std::pmr::vector<T>(other, &(this->m_pool)) {
        this->reserve(Capacity);
    }
    // Move constructor
    GrowFixedVector(GrowFixedVector&& other) noexcept
        : GrowFixedVectorStorage<T, Capacity>(), std::pmr::vector<T>(std::move(other), &(this->m_pool)) {
        this->reserve(Capacity);
    }
    // Copy assigment
    GrowFixedVector& operator=(const GrowFixedVector& other) {
        if (this != &other) {
            std::pmr::vector<T>::operator=(other);
        }
        return *this;
    }
    // Move assigment
    GrowFixedVector& operator=(GrowFixedVector&& other) noexcept {
        if (this != &other) {
            std::pmr::vector<T>::operator=(std::move(other));
        }
        return *this;
    }
};
}  // namespace XPN