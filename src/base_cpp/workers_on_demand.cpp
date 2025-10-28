
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

#include "workers_on_demand.hpp"
#include <thread> 

namespace XPN
{
    workers_on_demand::workers_on_demand(bool with_limits) 
    {
        if (with_limits){
            m_num_threads = std::thread::hardware_concurrency() * 2;
        }else{
            m_num_threads = INT32_MAX;
        }
    }
    workers_on_demand::~workers_on_demand() 
    {
        wait_all();
    }

    uint32_t workers_on_demand::size() const {
        return m_num_threads;
    }

    std::future<int> workers_on_demand::launch(std::function<int()> task)
    {
        {
            std::unique_lock<std::mutex> lock(m_wait_mutex);
            
            m_full_cv.wait(lock, [this] { 
                return m_wait < m_num_threads; 
            });

            m_wait++;
        }
        std::packaged_task<int()> p_task([this, task]{
            int result = task(); 

            {
                std::unique_lock<std::mutex> lock(m_wait_mutex); 
                m_wait--;
                if (m_wait == 0){
                    m_wait_cv.notify_one();
                }
                m_full_cv.notify_one();
            }

            return result;
        });

        std::future<int> result = p_task.get_future();
        std::thread t(std::move(p_task));
        t.detach();
        return result;
    }

    void workers_on_demand::launch_no_future(std::function<void()> task)
    {
        {
            std::unique_lock<std::mutex> lock(m_wait_mutex);
            
            m_full_cv.wait(lock, [this] { 
                return m_wait < m_num_threads; 
            });

            m_wait++;
        }
        std::thread t([this, task] { 
            task(); 

            {
                std::unique_lock<std::mutex> lock(m_wait_mutex); 
                m_wait--;
                if (m_wait == 0){
                    m_wait_cv.notify_one();
                }
                m_full_cv.notify_one();
            }
        });

        t.detach();
    }

    void workers_on_demand::wait_all() 
    {
        std::unique_lock<std::mutex> lock(m_wait_mutex);
        
        m_wait_cv.wait(lock, [this] { 
            return m_wait == 0;
        });
    }
} // namespace XPN