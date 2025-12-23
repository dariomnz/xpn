
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

#include <atomic>
#include <condition_variable>
#include <mutex>

namespace XPN {

// Like std::future but without allocations
template <typename T>
struct TaskResult {
    T value;

    enum state_t {
        UNINITIALIZED,
        WAITING,
        READY,
    };
    std::atomic<state_t> state{UNINITIALIZED};

    TaskResult() = default;
    ~TaskResult() {
        if (state.load() == WAITING) {
            state.wait(WAITING);
        }
    }
    // Delete copy constructor
    TaskResult(const TaskResult&) = delete;
    // Delete copy assignment operator
    TaskResult& operator=(const TaskResult&) = delete;
    // Delete move constructor
    TaskResult(TaskResult&&) = delete;
    // Delete move assignment operator
    TaskResult& operator=(TaskResult&&) = delete;

    void reset() { state.store(UNINITIALIZED); }

    void init() { state.store(WAITING); }

    void set_value(T&& v) {
        value = std::move(v);
        state.store(READY);
        state.notify_all();
    }

    bool is_valid() { return state.load() != UNINITIALIZED; }

    T get() {
        if (state.load() == WAITING) {
            state.wait(WAITING);
        }
        return std::move(value);
    }
};
}  // namespace XPN