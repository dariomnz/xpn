
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

#include "allocator.hpp"

#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <new>
#include <stdexcept>

#include "base_cpp/debug.hpp"

namespace XPN {

void ArenaAllocatorStorage::activate_arena(uint8_t* buffer, uint64_t size) {
    debug_info_fmt("activate_arena (%p-%p) with %ld size", buffer, buffer + size, size);
    if (buffer == nullptr || size == 0) throw std::bad_alloc();

    // Save the current arena and setup the new
    m_arena_buffer_stack[m_n_stack] = m_arena_buffer;
    m_arena_size_stack[m_n_stack] = m_arena_size;
    m_arena_pos_stack[m_n_stack] = m_arena_pos;
    m_n_stack++;

    m_arena_buffer = buffer;
    m_arena_size = size;
    m_arena_pos = 0;
    m_activated_arena = true;
    reset_stat_usage();

#ifdef DEBUG
    debug_dump_stack();
#endif
}

void ArenaAllocatorStorage::reset_arena() {
    debug_info_fmt("reset_arena %.6f %% used", ((float)m_arena_pos / (float)m_arena_size * 100.0f));
    print_stat_usage();
    reset_stat_usage();
    m_arena_pos = 0;
    m_activated_arena = true;
}

void ArenaAllocatorStorage::desactivate_all_arena() {
    debug_info_fmt("desactivate_all_arena");
    while (desactivate_arena()) {
    }
}

bool ArenaAllocatorStorage::desactivate_arena() {
    debug_info_fmt("desactivate_arena %.6f %% used", ((float)m_arena_pos / (float)m_arena_size * 100.0f));
    print_stat_usage();
    reset_stat_usage();

    if (m_n_stack <= 0) {
        return false;
    }
    m_n_stack--;
    m_arena_buffer = m_arena_buffer_stack[m_n_stack];
    m_arena_size = m_arena_size_stack[m_n_stack];
    m_arena_pos = m_arena_pos_stack[m_n_stack];

    // Activated only when there are a buffer assigned
    m_activated_arena = m_arena_buffer != nullptr;
    return true;
}

bool ArenaAllocatorStorage::is_active() { return m_activated_arena; }

void* ArenaAllocatorStorage::allocate(uint64_t size) {
    constexpr uint64_t DEFAULT_ALIGNMENT = sizeof(void*);
    auto under = size % DEFAULT_ALIGNMENT;
    auto adjust_size = DEFAULT_ALIGNMENT - under;
    auto adjusted_size = size + adjust_size;
    void* ptr = m_arena_buffer + m_arena_pos;

    if (m_arena_pos + adjusted_size <= m_arena_size) {
        m_arena_pos += adjusted_size;
        save_ptr(ptr, adjusted_size);
        add_usage_malloc_arena(adjusted_size);
    } else {
        fprintf(stderr, "Please asign more space to the arena, want to allocate %lu but free arena size are %lu\n",
                adjusted_size, m_arena_size - m_arena_pos);
        fflush(stderr);
        throw std::runtime_error("Please asign more space to the arena");
    }
    return ptr;
}

bool ArenaAllocatorStorage::deallocate(void* ptr) {
    auto sizeof_ptr = get_sizeof(ptr);
    if (sizeof_ptr > 0 || (m_activated_arena && ptr >= m_arena_buffer && ptr < (m_arena_buffer + m_arena_size))) {
        add_usage_free_arena(sizeof_ptr);
        delete_ptr(ptr);
        return true;
    }
    return false;
}

const char* ArenaAllocatorStorage::print_stat_usage() {
    debug_info_fmt("new memory alloc calls: %ld", m_malloc_calls);
    debug_info_fmt("new memory usage: %ld", m_memory_assigned);
    debug_info_fmt("new memory free calls: %ld", m_free_calls);
    debug_info_fmt("new memory freed: %ld", m_memory_freed);
    debug_info_fmt("new memory arena alloc calls: %ld", m_arena_malloc_calls);
    debug_info_fmt("new memory arena usage: %ld", m_arena_memory_assigned);
    debug_info_fmt("new memory arena free calls: %ld", m_arena_free_calls);
    debug_info_fmt("new memory arena freed: %ld", m_arena_memory_freed);
    return "";
}

void ArenaAllocatorStorage::reset_stat_usage() {
    m_malloc_calls = 0;
    m_memory_assigned = 0;
    m_free_calls = 0;
    m_memory_freed = 0;
    m_arena_malloc_calls = 0;
    m_arena_memory_assigned = 0;
    m_arena_free_calls = 0;
    m_arena_memory_freed = 0;
}

float ArenaAllocatorStorage::get_usage() { return ((float)m_arena_pos / (float)m_arena_size * 100.0f); }

void ArenaAllocatorStorage::save_ptr(void* ptr, uint64_t size) {
    if (m_n_ptrs >= MAX_PTRS) {
        fprintf(stderr, "Please set more size in the array of ptrs actual %d\n", MAX_PTRS);
        fflush(stderr);
        throw std::runtime_error("Please set more size in the array of ptrs actual");
    }
    m_ptrs[m_n_ptrs] = ptr;
    m_sizes[m_n_ptrs] = size;
    m_n_ptrs++;
}

void ArenaAllocatorStorage::delete_ptr(void* ptr) {
    int64_t idx = -1;
    if (ptr == nullptr) return;
    for (uint64_t i = 0; i < m_n_ptrs; i++) {
        if (m_ptrs[i] == ptr) {
            idx = i;
            break;
        }
    }
    if (idx == -1) {
        fprintf(stderr, "ptr %p not found in ptrs\n", ptr);
        fflush(stderr);
        throw std::runtime_error("free_ptr");
    }
    // Flip last to remove the ptr from ptrs
    m_ptrs[idx] = m_ptrs[m_n_ptrs - 1];
    m_sizes[idx] = m_sizes[m_n_ptrs - 1];
    m_n_ptrs--;
}

int64_t ArenaAllocatorStorage::get_sizeof(void* ptr) {
    for (uint64_t i = 0; i < m_n_ptrs; i++) {
        if (m_ptrs[i] == ptr) {
            return m_sizes[i];
        }
    }
    return -1;
}

const char* ArenaAllocatorStorage::debug_dump_ptrs() {
    for (uint64_t i = 0; i < m_n_ptrs; i++) {
        debug_info_fmt("ptrs[%ld] ptr %p size %ld", i, m_ptrs[i], m_sizes[i]);
    }
    return "";
}

const char* ArenaAllocatorStorage::debug_dump_stack() {
    for (uint64_t i = 0; i < m_n_stack; i++) {
        debug_info_fmt("Stack[%ld] buff %p size %ld pos %ld", i, m_arena_buffer_stack[i], m_arena_size_stack[i],
                       m_arena_pos_stack[i]);
    }
    debug_info_fmt("Actual buff %p size %ld pos %ld", m_arena_buffer, m_arena_size, m_arena_pos);

    return "";
}

void ArenaAllocatorStorage::add_usage_malloc(uint64_t size) {
    m_malloc_calls++;
    m_memory_assigned += size;
}

void ArenaAllocatorStorage::add_usage_malloc_arena(uint64_t size) {
    m_arena_malloc_calls++;
    m_arena_memory_assigned += size;
}

void ArenaAllocatorStorage::add_usage_free(uint64_t size) {
    m_free_calls++;
    m_memory_freed += size;
}

void ArenaAllocatorStorage::add_usage_free_arena(uint64_t size) {
    m_arena_free_calls++;
    m_arena_memory_freed += size;
}
}  // namespace XPN