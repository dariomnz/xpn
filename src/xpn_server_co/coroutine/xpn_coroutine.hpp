
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

#include <aio.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/poll.h>
#include <sys/socket.h>

#include <coroutine>
#include <memory>
#include <vector>

#include "base_cpp/workers.hpp"
#include "lfi.h"
#include "lfi_async.h"
#include "lfi_error.h"
#include "xpn_server_ops.hpp"
#include "xpn_server_params.hpp"

namespace XPN {

template <typename T>
struct task {
    // The return type of a coroutine must contain a nested struct or type alias called `promise_type`
    struct promise_type {
        // Keep a coroutine handle referring to the parent coroutine if any. That is, if we
        // co_await a coroutine within another coroutine, this handle will be used to continue
        // working from where we left off.
        std::coroutine_handle<> precursor;

        // Place to hold the results produced by the coroutine
        T data;

        // Invoked when we first enter a coroutine. We initialize the precursor handle
        // with a resume point from where the task is ultimately suspended
        task get_return_object() noexcept { return {std::coroutine_handle<promise_type>::from_promise(*this)}; }

        // When the caller enters the coroutine, we have the option to suspend immediately.
        // Let's choose not to do that here
        std::suspend_never initial_suspend() const noexcept { return {}; }

        // If an exception was thrown in the coroutine body, we would handle it here
        void unhandled_exception() {}

        // The coroutine is about to complete (via co_return or reaching the end of the coroutine body).
        // The awaiter returned here defines what happens next
        auto final_suspend() const noexcept {
            struct awaiter {
                // Return false here to return control to the thread's event loop. Remember that we're
                // running on some async thread at this point.
                bool await_ready() const noexcept { return false; }

                void await_resume() const noexcept {}

                // Returning a coroutine handle here resumes the coroutine it refers to (needed for
                // continuation handling). If we wanted, we could instead enqueue that coroutine handle
                // instead of immediately resuming it by enqueuing it and returning void.
                std::coroutine_handle<> await_suspend(std::coroutine_handle<promise_type> h) noexcept {
                    auto precursor = h.promise().precursor;
                    if (precursor) {
                        return precursor;
                    }
                    return std::noop_coroutine();
                }
            };
            return awaiter{};
        }
        // When the coroutine co_returns a value, this method is used to publish the result
        void return_value(T value) noexcept { data = std::move(value); }
    };

    // The following methods make our task type conform to the awaitable concept, so we can
    // co_await for a task to complete

    bool await_ready() const noexcept {
        // No need to suspend if this task has no outstanding work
        return handle.done();
    }

    T await_resume() const noexcept {
        // The returned value here is what `co_await our_task` evaluates to
        return std::move(handle.promise().data);
    }

    void await_suspend(std::coroutine_handle<> coroutine) const noexcept {
        // The coroutine itself is being suspended (async work can beget other async work)
        // Record the argument as the continuation point when this is resumed later. See
        // the final_suspend awaiter on the promise_type above for where this gets used
        handle.promise().precursor = coroutine;
    }
    
    bool done() const {
        return handle.done();
    }

    // This handle is assigned to when the coroutine itself is suspended (see await_suspend above)
    std::coroutine_handle<promise_type> handle;
};

template <typename T>
static inline void remove_unordered(std::vector<T>& vec, size_t pos) {
    std::iter_swap(vec.begin() + pos, vec.end() - 1);
    vec.erase(vec.end() - 1);
}

template <typename K, typename V>
struct double_vector {
   private:
    std::vector<K> m_vec1;
    std::vector<V> m_vec2;

   public:
    std::mutex m_mutex;

    size_t size1() { return m_vec1.size(); }
    K* data1() { return m_vec1.data(); }
    size_t size2() { return m_vec1.size(); }
    K* data2() { return m_vec1.data(); }
    K get1(size_t pos) { return m_vec1[pos]; }
    V get2(size_t pos) { return m_vec2[pos]; }

    void append(K value1, V value2) {
        std::unique_lock lock(m_mutex);
        debug_info("Before append " << to_str());
        m_vec1.push_back(value1);
        m_vec2.push_back(value2);
        debug_info("After append " << to_str());
    }

    void remove(size_t pos) {
        std::unique_lock lock(m_mutex);
        debug_info("Before remove " << to_str());
        remove_unordered(m_vec1, pos);
        remove_unordered(m_vec2, pos);
        debug_info("After remove " << to_str());
    }

    std::string to_str(size_t pos) {
        std::stringstream out;
        if (pos < m_vec1.size()) {
            out << "[" << m_vec1[pos] << ", " << m_vec2[pos] << "] ";
        } else {
            out << "[out of bounds] ";
        }
        return out.str();
    }

    std::string to_str() {
        std::stringstream out;
        for (size_t i = 0; i < m_vec1.size(); i++) {
            out << "[" << m_vec1[i] << ", " << m_vec2[i] << "] ";
        }
        return out.str();
    }
};

class progress_loop {
   public:
    static progress_loop& get_instance() {
        static progress_loop loop;
        return loop;
    }

    void run_one_step() {
        // debug_info("run_one_step of progress_loop");
        constexpr int MAX_EVENTS = 1;
        epoll_event events[MAX_EVENTS];
        {
            int event_count = epoll_wait(m_epoll_fd, events, MAX_EVENTS, 0);
            if (event_count == -1) {
                perror("Error en epoll_wait");
                return;
            }
            for (int i = 0; i < event_count; ++i) {
                auto handle_ptr = events[i].data.ptr;
                debug_info("Completed a epoll coroutine " << handle_ptr);
                if (handle_ptr) {
                    debug_info("Resuming epoll coroutine " << handle_ptr);
                    m_worker->launch_no_future([handle_ptr]() {
                        debug_info("[" << std::this_thread::get_id() << "] resume epoll coroutine");
                        std::coroutine_handle<>::from_address(handle_ptr).resume();
                    });
                }
            }
        }
        {
            std::unique_lock lock(m_waiting_aio.m_mutex);
            int completed = -1;
            for (size_t i = 0; i < m_waiting_aio.size1(); i++) {
                int status = aio_error(m_waiting_aio.get1(i));

                if (status != EINPROGRESS) {
                    completed = i;
                    if (status != 0) {
                        print("Error on aio_error " << status << " " << strerror(status));
                    }
                    break;
                }
            }
            if (completed != -1) {
                auto handle_ptr = m_waiting_aio.get2(completed);
                debug_info("completed a aio coroutine " << m_waiting_aio.to_str(completed));
                lock.unlock();
                m_waiting_aio.remove(completed);
                if (handle_ptr) {
                    debug_info("Resuming aio coroutine " << handle_ptr);
                    m_worker->launch_no_future([handle_ptr]() {
                        debug_info("[" << std::this_thread::get_id() << "] resume aio coroutine");
                        std::coroutine_handle<>::from_address(handle_ptr).resume();
                    });
                }
            }
        }
        {
            std::unique_lock lock(m_waiting_req.m_mutex);
            if (m_waiting_req.size1() == 0) return;
            int completed = lfi_wait_any_t(m_waiting_req.data1(), m_waiting_req.size1(), 0);

            if (completed < 0) {
                if (completed != -LFI_TIMEOUT) {
                    std::cerr << "Error in lfi_wait_any " << completed << " " << lfi_strerror(completed);
                }
            } else {
                auto handle_ptr = m_waiting_req.get2(completed);
                debug_info("completed a lfi coroutine " << m_waiting_aio.to_str(completed));
                lock.unlock();
                m_waiting_req.remove(completed);
                if (handle_ptr) {
                    debug_info("Resuming lfi coroutine " << handle_ptr);
                    m_worker->launch_no_future([handle_ptr]() {
                        debug_info("[" << std::this_thread::get_id() << "] resume lfi coroutine");
                        std::coroutine_handle<>::from_address(handle_ptr).resume();
                    });
                }
            }
        }
    }

    int get_epoll_fd() const { return m_epoll_fd; }
    auto& get_waiting_req() { return m_waiting_req; }
    auto& get_waiting_aio() { return m_waiting_aio; }
    auto& get_worker() { return m_worker; }

   private:
    int m_epoll_fd;
    double_vector<lfi_request*, void*> m_waiting_req;
    double_vector<aiocb*, void*> m_waiting_aio;
    std::unique_ptr<workers> m_worker;

    progress_loop() {
        m_epoll_fd = epoll_create1(0);
        if (m_epoll_fd == -1) {
            perror("Error al crear epoll");
            exit(EXIT_FAILURE);
        }
        m_worker = workers::Create(workers_mode::sequential, false);
        // m_worker = workers::Create(workers_mode::thread_pool, false);
        // m_worker = workers::Create(workers_mode::thread_on_demand, false);
        aioinit aioinit_s = {};
        aioinit_s.aio_num = 256;
        aioinit_s.aio_idle_time = 10;
        aio_init(&aioinit_s);
    }

    ~progress_loop() { close(m_epoll_fd); }

    progress_loop(const progress_loop&) = delete;
    progress_loop& operator=(const progress_loop&) = delete;
    progress_loop(const progress_loop&&) = delete;
    progress_loop&& operator=(const progress_loop&&) = delete;
};

struct LFIReqAwaitable {
   protected:
    bool m_request_to_free = false;
    lfi_request* m_request = nullptr;
    int m_error = 0;

    LFIReqAwaitable() = delete;
    LFIReqAwaitable(lfi_request* request) : m_request(request) {}
    LFIReqAwaitable(int comm_id) {
        m_request = lfi_request_create(comm_id);
        m_request_to_free = true;
    }

   public:
    ~LFIReqAwaitable() {
        if (m_request_to_free) {
            lfi_request_free(m_request);
        }
    }

    bool await_ready() const noexcept {
        debug_info("LFIReqAwaitable ready");
        if (m_error < 0) return true;
        return lfi_request_completed(m_request);
    }

    void await_suspend(std::coroutine_handle<> handle) const noexcept {
        progress_loop::get_instance().get_waiting_req().append(m_request, handle.address());
        debug_info("LFIReqAwaitable stored and suspended " << handle.address());
    }

    ssize_t await_resume() const noexcept {
        debug_info("Resume LFIReqAwaitable");
        if (m_error < 0) return m_error;
        auto req_error = lfi_request_error(m_request);
        if (req_error < 0) return req_error;
        return lfi_request_size(m_request);
    }
};

struct LFIRecvAwaitable : public LFIReqAwaitable {
    LFIRecvAwaitable(int comm_id, void* buffer, size_t size, int tag = 0) : LFIReqAwaitable(comm_id) {
        m_error = lfi_trecv_async(m_request, buffer, size, tag);
    }
    LFIRecvAwaitable(lfi_request* request, void* buffer, size_t size, int tag = 0) : LFIReqAwaitable(request) {
        m_error = lfi_trecv_async(m_request, buffer, size, tag);
    }
};

struct LFISendAwaitable : public LFIReqAwaitable {
    LFISendAwaitable(int comm_id, const void* buffer, size_t size, int tag = 0) : LFIReqAwaitable(comm_id) {
        m_error = lfi_tsend_async(m_request, buffer, size, tag);
    }
    LFISendAwaitable(lfi_request* request, const void* buffer, size_t size, int tag = 0) : LFIReqAwaitable(request) {
        m_error = lfi_tsend_async(m_request, buffer, size, tag);
    }
};

struct LFIAcceptAwaitable {
    int m_server_id;
    int m_epoll_fd;

    LFIAcceptAwaitable(int server_id)
        : m_server_id(server_id), m_epoll_fd(progress_loop::get_instance().get_epoll_fd()) {}

    bool await_ready() const noexcept {
        debug_info("LFIAcceptAwaitable ready");
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle) noexcept {
        epoll_event event{};
        event.events = EPOLLIN | EPOLLONESHOT;
        event.data.ptr = handle.address();

        if (epoll_ctl(m_epoll_fd, EPOLL_CTL_MOD, m_server_id, &event) == -1) {
            if (epoll_ctl(m_epoll_fd, EPOLL_CTL_ADD, m_server_id, &event) == -1) {
                perror("Error al registrar socket de servidor en epoll");
                handle.destroy();
            }
        }
        debug_info("LFIAcceptAwaitable stored and suspended " << handle.address());
    }

    int await_resume() noexcept {
        debug_info("Resume LFIAcceptAwaitable");
        int clientSocket = lfi_server_accept(m_server_id);
        if (clientSocket == -1) {
            perror("Error al aceptar conexión");
        }
        return clientSocket;
    }
};

struct SocketAcceptAwaitable {
    int m_socket;
    int m_epoll_fd;

    SocketAcceptAwaitable(int socket) : m_socket(socket), m_epoll_fd(progress_loop::get_instance().get_epoll_fd()) {}

    bool await_ready() const noexcept {
        debug_info("SocketAcceptAwaitable ready");
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle) noexcept {
        epoll_event event{};
        event.events = EPOLLIN | EPOLLONESHOT;
        event.data.ptr = handle.address();

        if (epoll_ctl(m_epoll_fd, EPOLL_CTL_MOD, m_socket, &event) == -1) {
            if (epoll_ctl(m_epoll_fd, EPOLL_CTL_ADD, m_socket, &event) == -1) {
                perror("Error registering the socket in epoll");
                handle.destroy();
            }
        }
        debug_info("SocketAcceptAwaitable stored and suspended " << handle.address());
    }

    int await_resume() noexcept {
        debug_info("Resume SocketAcceptAwaitable");
        struct sockaddr_in client_addr;
        socklen_t sock_size = sizeof(sockaddr_in);
        int client_socket = PROXY(accept)(m_socket, (struct sockaddr*)&client_addr, &sock_size);
        if (client_socket == -1) {
            perror("Error al aceptar conexión");
        }
        return client_socket;
    }
};

struct EPollAwaitable {
   protected:
    int m_socket = -1;
    void* m_buff = nullptr;
    const void* m_c_buff = nullptr;
    size_t m_size = 0;
    int m_events = 0;
    int m_epoll_fd;

    EPollAwaitable(int socket, void* buff, size_t size, short events)
        : m_socket(socket),
          m_buff(buff),
          m_size(size),
          m_events(events),
          m_epoll_fd(progress_loop::get_instance().get_epoll_fd()) {}
    EPollAwaitable(int socket, const void* buff, size_t size, short events)
        : m_socket(socket),
          m_c_buff(buff),
          m_size(size),
          m_events(events),
          m_epoll_fd(progress_loop::get_instance().get_epoll_fd()) {}

   public:
    bool await_ready() const noexcept {
        debug_info("EPollAwaitable ready");
        pollfd pfd;
        pfd.fd = m_socket;
        pfd.events = m_events;  // Convertir eventos epoll a poll
        pfd.revents = 0;
        if (poll(&pfd, 1, 0) > 0) {
            return (pfd.revents & m_events);
        }
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle) noexcept {
        epoll_event event{};
        event.events = m_events | EPOLLONESHOT;
        event.data.ptr = handle.address();

        if (epoll_ctl(m_epoll_fd, EPOLL_CTL_MOD, m_socket, &event) == -1) {
            if (epoll_ctl(m_epoll_fd, EPOLL_CTL_ADD, m_socket, &event) == -1) {
                perror("Error registering the socket in epoll");
                handle.destroy();
            }
        }
        debug_info("EPollAwaitable stored and suspended " << handle.address());
    }
};

struct SocketRecvAwaitable : public EPollAwaitable {
    SocketRecvAwaitable(int socket, void* buff, size_t size) : EPollAwaitable(socket, buff, size, EPOLLIN) {}
    ssize_t await_resume() noexcept {
        debug_info("Resume SocketRecvAwaitable");
        return PROXY(read)(m_socket, m_buff, m_size);
    }
};

struct SocketSendAwaitable : public EPollAwaitable {
    SocketSendAwaitable(int fd, const void* buff, size_t size) : EPollAwaitable(fd, buff, size, EPOLLOUT) {}
    ssize_t await_resume() noexcept {
        debug_info("Resume SocketSendAwaitable");
        return PROXY(write)(m_socket, m_c_buff, m_size);
    }
};

struct AIOAwaitable {
   protected:
    int m_fd = -1;
    void* m_buff = nullptr;
    const void* m_c_buff = nullptr;
    size_t m_size = 0;
    off_t m_offset = 0;
    aiocb m_aiocb = {};
    int m_error = 0;

    AIOAwaitable(int fd, void* buff, size_t size, off_t offset)
        : m_fd(fd), m_buff(buff), m_size(size), m_offset(offset) {}
    AIOAwaitable(int fd, const void* buff, size_t size, off_t offset)
        : m_fd(fd), m_c_buff(buff), m_size(size), m_offset(offset) {}
    AIOAwaitable(int fd) : m_fd(fd) {}

   public:
    bool await_ready() const noexcept {
        debug_info("AIOAwaitable ready");
        if (m_error < 0) return true;
        return aio_error(&m_aiocb) != EINPROGRESS;
    }

    void await_suspend(std::coroutine_handle<> handle) noexcept {
        progress_loop::get_instance().get_waiting_aio().append(&m_aiocb, handle.address());
        debug_info("AIOAwaitable stored and suspended " << handle.address());
    }

    int await_resume() noexcept {
        debug_info("Resume AIOAwaitable");
        if (m_error < 0) return m_error;
        return aio_return(&m_aiocb);
    }
};

struct AIOReadAwaitable : public AIOAwaitable {
    AIOReadAwaitable(int fd, void* buff, size_t size, off_t offset) : AIOAwaitable(fd, buff, size, offset) {
        int ret = 0;
        m_aiocb.aio_fildes = m_fd;
        m_aiocb.aio_buf = m_buff;
        m_aiocb.aio_nbytes = m_size;
        m_aiocb.aio_offset = m_offset;
        ret = aio_read(&m_aiocb);
        if (ret < 0) {
            m_error = ret;
            print_error("Error in aio_read");
        }
    }
};

struct AIOWriteAwaitable : public AIOAwaitable {
    AIOWriteAwaitable(int fd, const void* buff, size_t size, off_t offset) : AIOAwaitable(fd, buff, size, offset) {
        int ret = 0;
        m_aiocb.aio_fildes = m_fd;
        m_aiocb.aio_buf = const_cast<void*>(m_c_buff);
        m_aiocb.aio_nbytes = m_size;
        m_aiocb.aio_offset = m_offset;
        ret = aio_write(&m_aiocb);
        if (ret < 0) {
            m_error = ret;
            print_error("Error in aio_write");
        }
    }
};

struct AIOFsyncAwaitable : public AIOAwaitable {
    AIOFsyncAwaitable(int fd) : AIOAwaitable(fd) {
        int ret = 0;
        m_aiocb.aio_fildes = m_fd;
        ret = aio_fsync(O_SYNC, &m_aiocb);
        if (ret < 0) {
            m_error = ret;
            print_error("Error in aio_fsync");
        }
    }
};

class co_mutex {
   public:
    struct lock_awaiter {
        bool await_ready() const { return !m_co_mutex.m_flag.test_and_set(); }

        void await_suspend(std::coroutine_handle<> h) { m_co_mutex.m_queue_vec.push_back(h.address()); }

        void await_resume() {}

        co_mutex& m_co_mutex;
    };

    lock_awaiter lock() { return {*this}; }

    void unlock() {
        if (!m_queue_vec.empty()) {
            auto next = m_queue_vec[0];
            remove_unordered(m_queue_vec, 0);
            auto& m_worker = progress_loop::get_instance().get_worker();
            m_worker->launch_no_future([next]() {
                debug_info("[" << std::this_thread::get_id() << "] resume co_mutex");
                std::coroutine_handle<>::from_address(next).resume();
            });
        } else {
            m_flag.clear();
        }
    }

   private:
    std::atomic_flag m_flag;
    std::vector<void*> m_queue_vec;
};
}  // namespace XPN