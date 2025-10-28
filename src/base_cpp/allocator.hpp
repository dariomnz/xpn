
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
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#define USE_STD_NEW

namespace XPN {

struct ArenaAllocatorStorage {
   public:
    static ArenaAllocatorStorage& instance() {
        static ArenaAllocatorStorage instance;
        return instance;
    }
    void activate_arena(uint8_t* buffer, uint64_t size);
    void reset_arena();
    void desactivate_arena();

    bool is_active();
    void* allocate(uint64_t size);
    bool deallocate(void* ptr);

    void save_ptr(void* ptr, uint64_t size);
    void delete_ptr(void* ptr);
    int64_t get_sizeof(void* ptr);

    const char* print_stat_usage();
    void reset_stat_usage();
    float get_usage();

    void add_usage_malloc(uint64_t size);
    void add_usage_free(uint64_t size);
    void add_usage_malloc_arena(uint64_t size);
    void add_usage_free_arena(uint64_t size);

   private:
    uint64_t m_malloc_calls = 0;
    uint64_t m_memory_assigned = 0;
    uint64_t m_free_calls = 0;
    uint64_t m_memory_freed = 0;
    uint64_t m_arena_malloc_calls = 0;
    uint64_t m_arena_memory_assigned = 0;
    uint64_t m_arena_free_calls = 0;
    uint64_t m_arena_memory_freed = 0;
    bool m_activated_arena = false;
    uint8_t* m_arena_buffer = nullptr;
    uint64_t m_arena_size = 0;
    uint64_t m_arena_pos = 0;

    static constexpr int MAX_PTRS = 1024;
    uint64_t m_n_ptrs = 0;
    void* m_ptrs[MAX_PTRS] = {};
    uint64_t m_sizes[MAX_PTRS] = {};
    const char* debug_dump_ptrs();

    static constexpr int MAX_STACK = 64;
    uint64_t m_n_stack = 0;
    uint8_t* m_arena_buffer_stack[MAX_STACK] = {};
    uint64_t m_arena_size_stack[MAX_STACK] = {};
    uint64_t m_arena_pos_stack[MAX_STACK] = {};
    const char* debug_dump_stack();

   public:
    // For dmtcp
    int m_savedRestarts = 0;
    bool m_inCkpt = false;
};

class buffer_memory_resource {
   public:
    char* m_buffer;
    char* m_ptr;
    size_t m_size;

    explicit buffer_memory_resource(char* buffer, size_t size) : m_buffer(buffer), m_ptr(m_buffer), m_size(size) {
        // printf("buffer_memory_resource() (%p-%p) size %ld\n", m_buffer, m_buffer + m_size, m_size);
    }
    // Delete copy constructor
    buffer_memory_resource(const buffer_memory_resource& other) = delete;
    // Delete copy assignment operator
    buffer_memory_resource& operator=(const buffer_memory_resource&) = delete;
    // Delete move constructor
    buffer_memory_resource(buffer_memory_resource&&) = delete;
    // Delete move assignment operator
    buffer_memory_resource& operator=(buffer_memory_resource&&) = delete;

    ~buffer_memory_resource() {
        // printf("~buffer_memory_resource (%p-%p) size %ld used %ld\n", m_buffer, m_buffer + m_size, m_size,
        //        m_ptr - m_buffer);
    }

    void* allocate(std::size_t size) noexcept {
        void* ret = m_ptr;
        if (m_ptr + size > m_buffer + m_size) {
            printf("WARNING: cannot allocate %ld in the buffer %p with size %ld and free space %ld\n", size, m_buffer,
                   m_size, m_ptr - m_buffer);
            ret = nullptr;
        } else {
            m_ptr += size;
        }
        return ret;
    }

    void deallocate(void*) noexcept {}
};

template <typename T>
class buffer_allocator {
    buffer_memory_resource* m_res;

   public:
    using value_type = T;

    explicit buffer_allocator(buffer_memory_resource* resource) : m_res(resource) {
        // printf("buffer_allocator()\n");
    }

    buffer_allocator(const buffer_allocator& other) : m_res(other.m_res) {
        // printf("buffer_allocator(const buffer_allocator&)\n");
    }
    buffer_allocator(buffer_allocator&& other) : m_res(other.m_res) {
        // printf("buffer_allocator(buffer_allocator&&)\n");
    }

    // Delete copy assignment operator
    buffer_allocator& operator=(const buffer_allocator&) = delete;
    // Delete move assignment operator
    buffer_allocator& operator=(buffer_allocator&&) = delete;

    ~buffer_allocator() {
        // printf("~buffer_allocator\n");
    }

    T* allocate(std::size_t n) {
        auto ret = static_cast<T*>(m_res->allocate(sizeof(T) * n));
        // printf(">> allocate %p %ld\n", ret, n);
        // fflush(stdout);
        return ret;
    }
    void deallocate(T* ptr, [[maybe_unused]] std::size_t n) {
        m_res->deallocate(ptr);
        // printf("<< deallocate %p %ld\n", ptr, n);
        // fflush(stdout);
    }

    template <typename U>
    struct rebind {
        using other = buffer_allocator<U>;
    };

    friend bool operator==(const buffer_allocator& lhs, const buffer_allocator& rhs) { return lhs.m_res == rhs.m_res; }

    friend bool operator!=(const buffer_allocator& lhs, const buffer_allocator& rhs) { return lhs.m_res != rhs.m_res; }
};

using stack_ostringstream = std::basic_ostringstream<char, std::char_traits<char>, buffer_allocator<char>>;
using stack_string = std::basic_string<char, std::char_traits<char>, buffer_allocator<char>>;

inline stack_ostringstream make_stack_ostringstream(buffer_memory_resource* resource) {
    stack_string str(buffer_allocator<char>{resource});
    return stack_ostringstream{str};
}
}  // namespace XPN