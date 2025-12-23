
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
#include <functional>
#include <iostream>
#include <type_traits>
#include <utility>

namespace XPN {

// Helper to force the compiler to show the sizes in the error message
template <size_t ActualSize, size_t AllowedCapacity>
struct CheckSize {
    static constexpr bool value = ActualSize <= AllowedCapacity;
    static_assert(value, "ERROR: Captured lambda size exceeds FixedFunction capacity!");
};

template <typename Signature, size_t Capacity = 64>
class FixedFunction;

template <typename R, typename... Args, size_t Capacity>
class FixedFunction<R(Args...), Capacity> {
    // Internal structure for Type Erasure
    struct VTable {
        R (*call)(void*, Args&&...);
        void (*destroy)(void*);
        void (*copy)(const void*, void*);
        void (*move)(void*, void*);
    };

    // Aligned storage on the stack
    alignas(std::max_align_t) char storage[Capacity];
    const VTable* vtable = nullptr;

    void* ptr() { return static_cast<void*>(storage); }
    const void* ptr() const { return static_cast<const void*>(storage); }
    void reset() {
        if (vtable) {
            vtable->destroy(ptr());
            vtable = nullptr;
        }
    }

   public:
    FixedFunction() = default;

    template <typename F>
    FixedFunction(F f) {
        using FunctorType = typename std::decay<F>::type;

        static_assert(CheckSize<sizeof(FunctorType), Capacity>::value,
                      "Lambda capture is too large for FixedFunction. Increase Capacity.");
        static const VTable vt = {
            // Call logic
            [](void* src, Args&&... args) -> R {
                return (*static_cast<FunctorType*>(src))(std::forward<Args>(args)...);
            },
            // Destroy logic
            [](void* src) { static_cast<FunctorType*>(src)->~FunctorType(); },
            // Copy logic (Placement New Copy Constructor)
            [](const void* src, void* dest) { new (dest) FunctorType(*static_cast<const FunctorType*>(src)); },
            // Move logic (Placement New Move Constructor)
            [](void* src, void* dest) { new (dest) FunctorType(std::move(*static_cast<FunctorType*>(src))); }};

        vtable = &vt;
        new (storage) FunctorType(std::move(f));
    }

    // Destructor
    ~FixedFunction() {
        if (vtable) vtable->destroy(ptr());
    }

    // Copy Constructor
    FixedFunction(const FixedFunction& other) {
        if (other.vtable) {
            other.vtable->copy(other.ptr(), ptr());
            vtable = other.vtable;
        }
    }

    // Copy assigment
    FixedFunction& operator=(const FixedFunction& other) {
        if (this != &other) {
            reset();
            if (other.vtable) {
                other.vtable->copy(other.ptr(), ptr());
                vtable = other.vtable;
            }
        }
        return *this;
    }

    // Move Constructor
    FixedFunction(FixedFunction&& other) noexcept {
        if (other.vtable) {
            other.vtable->move(other.ptr(), ptr());
            vtable = other.vtable;
            other.reset();
        }
    }

    // Move assigment
    FixedFunction& operator=(FixedFunction&& other) noexcept {
        if (this != &other) {
            reset();
            if (other.vtable) {
                other.vtable->move(other.ptr(), ptr());
                vtable = other.vtable;
            }
        }
        return *this;
    }

    // Call Operator
    R operator()(Args... args) const {
        if (!vtable) throw std::bad_function_call();
        return vtable->call(const_cast<void*>(ptr()), std::forward<Args>(args)...);
    }

    // Check if the function is valid
    explicit operator bool() const noexcept { return vtable != nullptr; }
};
}  // namespace XPN