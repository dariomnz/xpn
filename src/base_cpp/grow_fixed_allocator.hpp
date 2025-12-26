
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

#include <cstddef>

namespace XPN {

template <size_t CapacityBytes>
struct InternalArena {
    char *ptr;
    char buffer[CapacityBytes];

    InternalArena() : ptr(buffer) {}

    char *allocate(size_t n) {
        if (ptr + n <= buffer + CapacityBytes) {
            char *r = ptr;
            ptr += n;
            return r;
        }
        return static_cast<char *>(::operator new(n));
    }

    void deallocate(char *p, [[maybe_unused]] size_t n) {
        if (p < buffer || p >= buffer + CapacityBytes) {
            ::operator delete(p);
        }
    }
};

template <typename T, size_t Capacity>
struct GrowFixedAllocator {
    using value_type = T;
    static constexpr size_t ArenaSize = Capacity * sizeof(T);
    InternalArena<ArenaSize> &arena;

    GrowFixedAllocator(InternalArena<ArenaSize> &a) : arena(a) {}

    template <typename U>
    GrowFixedAllocator(const GrowFixedAllocator<U, Capacity> &other) noexcept
        : arena(reinterpret_cast<InternalArena<ArenaSize> &>(other.arena)) {}

    template <typename U>
    struct rebind {
        using other = GrowFixedAllocator<U, Capacity>;
    };
    T *allocate(size_t n) { return reinterpret_cast<T *>(arena.allocate(n * sizeof(T))); }

    void deallocate(T *p, size_t n) { arena.deallocate(reinterpret_cast<char *>(p), n * sizeof(T)); }

    bool operator==(const GrowFixedAllocator &other) const noexcept { return &arena == &other.arena; }
    bool operator!=(const GrowFixedAllocator &other) const noexcept { return !(*this == other); }
};
}  // namespace XPN