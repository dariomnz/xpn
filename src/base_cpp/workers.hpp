
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

#include <memory>
#include <functional>
#include <future>
#include "fixed_function.hpp"
#include "task_result.hpp"

namespace XPN
{
    enum class workers_mode
    {
        sequential = 0,
        thread_pool = 1,
        thread_on_demand = 2,
    };

    struct WorkerResult {
        int result = 0;
        int errorno = 0;

        WorkerResult() = default;
        explicit WorkerResult(int res) : result(res), errorno(errno) {}
    };
    
    class workers
    {
    public:
        virtual ~workers() = default;

        virtual void launch(FixedFunction<WorkerResult()> task, TaskResult<WorkerResult>& result) = 0;
        virtual void launch_no_future(FixedFunction<void()> task) = 0;
        virtual void wait_all() = 0;
        virtual uint32_t size() const = 0;
    public:
        static std::unique_ptr<workers> Create(workers_mode mode, bool with_limits = true);
    };
} // namespace XPN
