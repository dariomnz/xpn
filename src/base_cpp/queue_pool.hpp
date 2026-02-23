
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

#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>

template <typename T>
class queue_pool {
   public:
    std::unique_ptr<T> acquire() {
        std::unique_lock lock(m_mutex);
        if (m_queue.empty()) {
            return std::make_unique<T>();
        }
        std::unique_ptr<T> obj = std::move(m_queue.back());
        m_queue.pop_back();
        return obj;
    }

    void release(std::unique_ptr<T> obj) {
        std::unique_lock lock(m_mutex);
        m_queue.emplace_back(std::move(obj));
    }

   private:
    std::vector<std::unique_ptr<T>> m_queue;
    std::mutex m_mutex;
};