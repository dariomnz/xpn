
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
    void desactivate_all_arena();
    bool desactivate_arena();

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

    static constexpr int MAX_PTRS = 8192;
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
}  // namespace XPN