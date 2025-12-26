
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

#include <vector>

#include "grow_fixed_allocator.hpp"

namespace XPN {
template <typename T, size_t Capacity>
struct GrowFixedVectorStorage {
    static constexpr size_t ArenaSize = Capacity * sizeof(T);
    InternalArena<ArenaSize> m_arena;
    GrowFixedAllocator<T, Capacity> m_allocator;

    // Default contructor
    GrowFixedVectorStorage() : m_arena(), m_allocator(m_arena) {}
    // Copy contructor
    GrowFixedVectorStorage(const GrowFixedVectorStorage&) : GrowFixedVectorStorage() {}
    // Move contructor
    GrowFixedVectorStorage(GrowFixedVectorStorage&&) noexcept : GrowFixedVectorStorage() {}
};

template <typename T, size_t Capacity>
class GrowFixedVector : private GrowFixedVectorStorage<T, Capacity>,
                        public std::vector<T, GrowFixedAllocator<T, Capacity>> {
    using BaseStorage = GrowFixedVectorStorage<T, Capacity>;
    using BaseVector = std::vector<T, GrowFixedAllocator<T, Capacity>>;

   public:
    // Default contructor
    GrowFixedVector() : BaseStorage(), BaseVector(BaseStorage::m_allocator) { this->reserve(Capacity); }

    // Copy contructor
    GrowFixedVector(const GrowFixedVector& other) : BaseStorage(), BaseVector(BaseStorage::m_allocator) {
        this->reserve(Capacity);
        this->assign(other.begin(), other.end());
    }
    // Move constructor
    GrowFixedVector(GrowFixedVector&& other) noexcept : BaseStorage(), BaseVector(BaseStorage::m_allocator) {
        this->reserve(Capacity);
        this->assign(other.begin(), other.end());
    }
    // Copy assigment
    GrowFixedVector& operator=(const GrowFixedVector& other) {
        if (this != &other) {
            BaseVector::operator=(other);
        }
        return *this;
    }
    // Move assigment
    GrowFixedVector& operator=(GrowFixedVector&& other) noexcept {
        if (this != &other) {
            BaseVector::operator=(std::move(other));
        }
        return *this;
    }
};
}  // namespace XPN