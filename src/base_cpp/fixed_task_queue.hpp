
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

#include "base_cpp/workers.hpp"
#include "task_result.hpp"

namespace XPN {

// Helpers to instanciate template types
namespace internal {

template <typename T>
struct arg_extractor : arg_extractor<decltype(&T::operator())> {};

// Lambdas with const
template <typename C, typename R, typename Arg>
struct arg_extractor<R (C::*)(Arg) const> {
    using type = std::decay_t<Arg>;
};

// Mutable lambdas
template <typename C, typename R, typename Arg>
struct arg_extractor<R (C::*)(Arg)> {
    using type = std::decay_t<Arg>;
};
}  // namespace internal

template <typename T, typename Handler, size_t Capacity = 64>
class FixedTaskQueue {
   private:
    TaskResult<T> m_storage[Capacity];  // Ring buffer

    size_t m_head = 0;                  // Where we produce
    size_t m_tail = 0;                  // Where we consume
    size_t m_count = 0;                 // The current count of elements

    workers& m_worker;                  // Worker where the task is run
    Handler m_on_result;                // Function to call when a result is consumed

   public:
    FixedTaskQueue(workers& worker, Handler on_result) : m_worker(worker), m_on_result(std::move(on_result)) {}
    ~FixedTaskQueue() {
        // If the queue is destroyed, we MUST wait for all pending tasks.
        // Otherwise, worker threads will write to a destroyed TaskResult.
        while (m_count > 0) {
            consume_one();
        }
    }
    // Delete copy constructor
    FixedTaskQueue(const FixedTaskQueue&) = delete;
    // Delete copy assignment operator
    FixedTaskQueue& operator=(const FixedTaskQueue&) = delete;
    // Delete move constructor
    FixedTaskQueue(FixedTaskQueue&&) = delete;
    // Delete move assignment operator
    FixedTaskQueue& operator=(FixedTaskQueue&&) = delete;

    // Attempts to launch a task. Automatically manages backpressure when the queue is full.
    // Returns false if an error was detected in a previous task and the loop must stop.
    template <typename Func>
    bool launch(Func&& task_func) {
        // 1. If the queue is full, consume one result to make room
        if (full()) {
            auto res = consume_one();
            if (!m_on_result(res)) return false;  // Error encountered, signal to break the loop
        }

        // 2. Launch the new task
        auto& slot = get_next_slot();
        m_worker.launch(std::forward<Func>(task_func), slot);
        return true;
    }

    // Waits for all remaining tasks in the queue to complete
    bool wait_remaining() {
        while (!empty()) {
            auto res = consume_one();
            if (!m_on_result(res)) return false;
        }
        return true;
    }

   private:
    // This is called when you want to launch a task
    TaskResult<T>& get_next_slot() {
        if (full()) {
            throw std::runtime_error("The StaticTaskQueue get_next_slot needs to be call making sure it is not full");
        }

        TaskResult<T>& slot = m_storage[m_head];

        // Reset the task for the recycled slot
        slot.reset();

        m_head = (m_head + 1) % Capacity;
        m_count++;

        return slot;
    }

    // This is called to get the result of the oldest task
    T consume_one() {
        // This shouldn't throw if used correctly in a loop
        if (m_count == 0) throw std::runtime_error("No tasks to consume");

        TaskResult<T>& slot = m_storage[m_tail];

        T val = slot.get();

        m_tail = (m_tail + 1) % Capacity;
        m_count--;

        return val;
    }

    bool full() const { return m_count == Capacity; }
    bool empty() const { return m_count == 0; }

    size_t count() const { return m_count; }
};

template <typename Handler>
FixedTaskQueue(workers&, Handler) -> FixedTaskQueue<typename internal::arg_extractor<Handler>::type, Handler>;
}  // namespace XPN