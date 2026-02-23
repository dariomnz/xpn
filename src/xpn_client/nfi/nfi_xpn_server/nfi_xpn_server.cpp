
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

#include <assert.h>
#include "nfi/nfi_server.hpp"
#include "nfi_xpn_server.hpp"
#include "lz4.h"
#include "base_cpp/timer.hpp"
#include "xpn/xpn_file.hpp"
#include "base_cpp/debug.hpp"
#include "base_cpp/xpn_env.hpp"
#include "xpn_server/xpn_server_ops.hpp"
#include <bits/chrono.h>
#include <fcntl.h>
#include <chrono>
#include <cstring>
#include <mutex>

#include "../nfi_sck_server/nfi_sck_server_comm.hpp"
#include "xpn_server/xpn_server_params.hpp"
#ifdef ENABLE_MQTT_SERVER
#include "../nfi_mqtt_server/nfi_mqtt_server_comm.hpp"
#endif

namespace XPN
{

static inline uint32_t concatenate_path(char *dest, std::string_view str1, std::string_view str2) {
  uint32_t length = 0;
  char *current_dest = dest;
  // Copy str1
  std::memcpy(current_dest, str1.data(), str1.size());
  length += str1.size();
  current_dest = dest + length;
  // Copy the separator
  current_dest[0] = '/';
  length += 1;
  current_dest = dest + length;
  // Copy str2
  std::memcpy(current_dest, str2.data(), str2.size());
  length += str2.size();
  current_dest = dest + length;
  // The \0 termination
  current_dest[0] = '\0';
  length += 1;
  return length;
}

// File API
int nfi_xpn_server::nfi_open (std::string_view path, int flags, mode_t mode, xpn_fh &fho)
{
  int ret;
  st_xpn_server_path_flags msg{};
  st_xpn_server_status status{};

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_open] >> Begin");

  uint32_t length = concatenate_path(msg.path.path, m_path, path);
  msg.path.size = length;
  msg.flags = flags;
  msg.mode = mode;
  msg.xpn_session = xpn_env::get_instance().xpn_session_file;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_open] nfi_xpn_server_open("<<msg.path.path<<", "<<flags<<", "<<mode<<")");
  
  ret = nfi_do_request(xpn_server_ops::OPEN_FILE, msg, status);
  if (status.ret < 0 || ret < 0){ 
    errno = status.server_errno;
    debug_error("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_open] ERROR: remote open fails to open '"<<msg.path.path<<"'");
    return -1;
  }

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_open] nfi_xpn_server_open("<<msg.path.path<<")="<<status.ret);
  
  fho.type = xpn_fh::type_t::File;
  fho.as.file.fd = status.ret;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_open] >> End");

  return 0;
}

int nfi_xpn_server::nfi_create (std::string_view path, mode_t mode, xpn_fh &fho)
{
  //NOTE: actualy creat is not in use, it use like POSIX open(path, O_WRONLY|O_CREAT|O_TRUNC, mode);
  return nfi_open(path, O_WRONLY|O_CREAT|O_TRUNC, mode, fho);
}

int nfi_xpn_server::nfi_close (std::string_view path, const xpn_fh &fh)
{
  bool is_mqtt = false;
  if (m_comm->m_type == server_type::SCK) {
      auto sck_comm = static_cast<nfi_sck_server_comm*>(m_comm.get());
      if (sck_comm->m_mqtt) {
        is_mqtt = true;
      }
  }
  if (xpn_env::get_instance().xpn_session_file == 1 || is_mqtt){
    st_xpn_server_close msg{};
    st_xpn_server_status status{};

    debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_close] >> Begin");

    debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_close] nfi_xpn_server_close("<<fh.as.file.fd<<")");

    msg.fd = fh.as.file.fd;
    msg.xpn_session = xpn_env::get_instance().xpn_session_file;
    // Only pass the path in mqtt
    if (is_mqtt) {
      uint32_t length = concatenate_path(msg.path.path, m_path, path);
      msg.path.size = length;
    } else {
      msg.path.size = 0;
    }

    nfi_do_request(xpn_server_ops::CLOSE_FILE, msg, status);

    if (status.ret < 0){
      errno = status.server_errno;
    }

    debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_close] nfi_xpn_server_close("<<fh.as.file.fd<<")="<<status.ret);
    debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_close] >> End");
    
    return status.ret;
  }else{
    // Without sesion close do nothing
    return 0;
  }
}

int64_t nfi_xpn_server::nfi_read_v1(std::string_view path, const xpn_fh &fh, char *buffer, int64_t offset, uint64_t size)
{
    char xpn_compression = xpn_env::get_instance().xpn_compression;
    uint64_t remaining = size;
    int64_t current_offset = offset;
    int64_t total_read = 0;
    bool must_compress = m_read_compressor.should_compress(size);

    debug_info("[SERV_ID=" << m_server << "] [NFI_XPN] [nfi_xpn_server_read] >> Begin V1");

    do {
        uint64_t chunk_size = must_compress ? std::min(remaining, (uint64_t)MAX_BUFFER_SIZE) : remaining;
        st_xpn_server_rw msg{};
        st_xpn_server_rw_req req{};
        
        uint32_t length = concatenate_path(msg.path.path, m_path, path);
        msg.path.size = length;
        msg.offset = current_offset;
        msg.size = chunk_size;
        msg.fd = fh.as.file.fd;
        msg.xpn_session = xpn_env::get_instance().xpn_session_file;
        msg.compressed_size = must_compress ? 1 : 0;
        msg.xpn_compression = xpn_compression;

        debug_info("[SERV_ID=" << m_server << "] [NFI_XPN] [nfi_xpn_server_read] chunk(" << msg.path.path << ", " << current_offset << ", " << chunk_size << ")");

        if (nfi_write_operation(xpn_server_ops::READ_FILE, msg) < 0) {
            debug_error("[SERV_ID=" << m_server << "] [NFI_XPN] [nfi_xpn_server_read] ERROR: nfi_write_operation fails");
            return -1;
        }

        std::chrono::time_point<std::chrono::high_resolution_clock> net_t1, net_t2, decom_t1, decom_t2;
        if (xpn_compression == 1) net_t1 = std::chrono::high_resolution_clock::now();

        if (m_comm->read_data(&req, sizeof(req)) < 0) {
            debug_error("[SERV_ID=" << m_server << "] [NFI_XPN] [nfi_xpn_server_read] ERROR: nfi_xpn_server_comm_read_data fails");
            return -1;
        }

        if (req.status.ret < 0) {
            errno = req.status.server_errno;
            debug_error("[SERV_ID=" << m_server << "] [NFI_XPN] [nfi_xpn_server_read] ERROR: req.status.ret " << req.status.ret << " fails " << strerror(errno));
            return -1;
        }

        if (req.size > 0) {
            if (req.compressed_size > 0) {
                char compressed_data[LZ4_COMPRESSBOUND(MAX_BUFFER_SIZE)];
                if (m_comm->read_data(compressed_data, req.compressed_size) < 0) return -1;
                
                if (xpn_compression == 1) net_t2 = std::chrono::high_resolution_clock::now();
                if (xpn_compression == 1) decom_t1 = std::chrono::high_resolution_clock::now();
                
                int decomp_ret = LZ4_decompress_safe(compressed_data, buffer + (size - remaining), req.compressed_size, req.uncompressed_size);
                
                if (xpn_compression == 1) decom_t2 = std::chrono::high_resolution_clock::now();

                if (decomp_ret < 0) {
                    debug_error("[SERV_ID=" << m_server << "] [NFI_XPN] [nfi_xpn_server_read] ERROR: LZ4_decompress_safe fails");
                    return -1;
                }

                if (xpn_compression == 1) {
                    m_read_compressor.update_metrics_comp(req.uncompressed_size, req.compressed_size,
                        std::chrono::duration_cast<std::chrono::microseconds>(net_t2 - net_t1), std::chrono::microseconds(req.rw_time_us),
                        std::chrono::microseconds(req.compress_time_us), std::chrono::duration_cast<std::chrono::microseconds>(decom_t2 - decom_t1));
                }
            } else {
                if (m_comm->read_data(buffer + (size - remaining), req.size) < 0) return -1;
                if (xpn_compression == 1) {
                    net_t2 = std::chrono::high_resolution_clock::now();
                    m_read_compressor.update_metrics(req.size, std::chrono::duration_cast<std::chrono::microseconds>(net_t2 - net_t1), std::chrono::microseconds(req.rw_time_us));
                }
            }
        }

        total_read += req.size;
        remaining -= chunk_size;
        current_offset += chunk_size;

    } while (remaining > 0);

    return total_read;
}

int64_t nfi_xpn_server::nfi_read_v2(std::string_view path, const xpn_fh &fh, char *buffer, int64_t offset, uint64_t size)
{
    char xpn_compression = xpn_env::get_instance().xpn_compression;
    uint64_t remaining = size;
    int64_t current_offset = offset;
    int64_t total_read = 0;
    bool must_compress = m_read_compressor.should_compress(size);

    debug_info("[SERV_ID=" << m_server << "] [NFI_XPN] [nfi_xpn_server_read] >> Begin V2");

    do {
        uint64_t chunk_size = must_compress ? std::min(remaining, (uint64_t)MAX_BUFFER_SIZE) : remaining;
        st_xpn_server_read_v2 msg{};
        st_xpn_server_read_v2_req req{};

        uint32_t length = concatenate_path(msg.path.path, m_path, path);
        msg.path.size = length;
        msg.offset = current_offset;
        msg.size = chunk_size;
        msg.fd = fh.as.file.fd;
        msg.xpn_session = xpn_env::get_instance().xpn_session_file;
        msg.should_compressed = must_compress ? 1 : 0;
        msg.xpn_compression = xpn_compression;

        if (nfi_write_operation(xpn_server_ops::READ_FILE_V2, msg) < 0) {
            debug_error("[SERV_ID=" << m_server << "] [NFI_XPN] [nfi_xpn_server_read] ERROR: nfi_write_operation fails");
            return -1;
        }

        std::chrono::time_point<std::chrono::high_resolution_clock> net_t1, net_t2, decom_t1, decom_t2;
        if (xpn_compression == 1) net_t1 = std::chrono::high_resolution_clock::now();

        if (msg.should_compressed == 1) {
            char compressed_data[LZ4_COMPRESSBOUND(MAX_BUFFER_SIZE)];
            struct iovec iovs[2] = {
                {.iov_base = &req, .iov_len = sizeof(req)},
                {.iov_base = compressed_data, .iov_len = sizeof(compressed_data)}
            };
            if (m_comm->readv_data(iovs, 2) < 0) return -1;

            if (xpn_compression == 1) net_t2 = std::chrono::high_resolution_clock::now();
            if (xpn_compression == 1) decom_t1 = std::chrono::high_resolution_clock::now();

            int decomp_ret = LZ4_decompress_safe(compressed_data, buffer + (size - remaining), req.compressed_size, req.uncompressed_size);

            if (xpn_compression == 1) decom_t2 = std::chrono::high_resolution_clock::now();

            if (decomp_ret < 0) {
                debug_error("[SERV_ID=" << m_server << "] [NFI_XPN] [nfi_xpn_server_read] ERROR: LZ4_decompress_safe fails");
                return -1;
            }

            if (xpn_compression == 1) {
                m_read_compressor.update_metrics_comp(req.uncompressed_size, req.compressed_size,
                    std::chrono::duration_cast<std::chrono::microseconds>(net_t2 - net_t1), std::chrono::microseconds(req.read_time_us),
                    std::chrono::microseconds(req.compress_time_us), std::chrono::duration_cast<std::chrono::microseconds>(decom_t2 - decom_t1));
            }
        } else {
            struct iovec iovs[2] = {
                {.iov_base = &req, .iov_len = sizeof(req)},
                {.iov_base = buffer + (size - remaining), .iov_len = chunk_size}
            };
            if (m_comm->readv_data(iovs, 2) < 0) return -1;
            if (xpn_compression == 1) {
                net_t2 = std::chrono::high_resolution_clock::now();
                m_read_compressor.update_metrics(chunk_size, std::chrono::duration_cast<std::chrono::microseconds>(net_t2 - net_t1), std::chrono::microseconds(req.read_time_us));
            }
        }

        if (req.status.ret < 0) {
            errno = req.status.server_errno;
            return -1;
        }

        total_read += req.size;
        remaining -= chunk_size;
        current_offset += chunk_size;

    } while (remaining > 0);

    return total_read;
}

int64_t nfi_xpn_server::nfi_read(std::string_view path, const xpn_fh &fh, char *buffer, int64_t offset, uint64_t size)
{
    if (size == 0) return 0;

    std::optional<std::unique_lock<std::mutex>> lock = std::nullopt;
    if (!xpn_env::get_instance().xpn_connect && m_comm == nullptr) {
        m_comm = m_control_comm_connectionless->connect(m_server, m_connectionless_port);
    } else if (m_comm->m_type == server_type::SCK) {
        auto sck_comm = static_cast<nfi_sck_server_comm*>(m_comm.get());
        lock = std::unique_lock<std::mutex>(sck_comm->m_mutex);
        debug_info("lock sck comm mutex");
    }

    int64_t ret;

    if (xpn_env::get_instance().xpn_rw_v2 == 0 || m_protocol_type != nfi_server::protocol_t::fabric) {
        ret = nfi_read_v1(path, fh, buffer, offset, size);
    } else {
        ret = nfi_read_v2(path, fh, buffer, offset, size);
    }

    if (!xpn_env::get_instance().xpn_connect) {
        m_control_comm_connectionless->disconnect(m_comm);
        m_comm = nullptr;
    }

    debug_info("[SERV_ID=" << m_server << "] [NFI_XPN] [nfi_xpn_server_read] >> End");
    return ret;
}

int64_t nfi_xpn_server::nfi_write_v1(std::string_view path, const xpn_fh &fh, const char *uncompressed_buffer, int64_t offset, uint64_t uncompressed_size)
{
    if (uncompressed_size == 0) return 0;

    char xpn_compression = xpn_env::get_instance().xpn_compression;
    uint64_t remaining = uncompressed_size;
    int64_t current_offset = offset;
    int64_t total_written = 0;
    bool must_compress = m_write_compressor.should_compress(uncompressed_size);

    debug_info("[SERV_ID=" << m_server << "] [NFI_XPN] [nfi_xpn_server_write] >> Begin V1");

    do {
        uint64_t chunk_size = must_compress ? std::min(remaining, (uint64_t)MAX_BUFFER_SIZE) : remaining;
        st_xpn_server_rw msg{};
        st_xpn_server_rw_req req{};
        char compressed_data[LZ4_COMPRESSBOUND(MAX_BUFFER_SIZE)];
        int compressed_data_size = 0;

        std::chrono::time_point<std::chrono::high_resolution_clock> com_t1, com_t2, net_t1, net_t2;

        if (must_compress) {
            if (xpn_compression == 1) com_t1 = std::chrono::high_resolution_clock::now();
            compressed_data_size = LZ4_compress_fast(uncompressed_buffer + (uncompressed_size - remaining), 
                                                   compressed_data, chunk_size, sizeof(compressed_data), 10);
            if (compressed_data_size < 0) {
                debug_error("[SERV_ID=" << m_server << "] [NFI_XPN] [nfi_xpn_server_write] ERROR: LZ4_compress_fast fails");
                return -1;
            }
            if (xpn_compression == 1) com_t2 = std::chrono::high_resolution_clock::now();
        }

        uint32_t length = concatenate_path(msg.path.path, m_path, path);
        msg.path.size = length;
        msg.offset = current_offset;
        msg.uncompressed_size = chunk_size;
        msg.compressed_size = compressed_data_size;
        msg.fd = fh.as.file.fd;
        msg.xpn_session = xpn_env::get_instance().xpn_session_file;
        msg.xpn_compression = xpn_compression;

        debug_info("[SERV_ID=" << m_server << "] [NFI_XPN] [nfi_xpn_server_write] chunk(" 
                   << msg.path.path << ", " << current_offset << ", " << chunk_size << ")");

        if (nfi_write_operation(xpn_server_ops::WRITE_FILE, msg) < 0) {
            debug_error("[SERV_ID=" << m_server << "] [NFI_XPN] [nfi_xpn_server_write] ERROR: nfi_write_operation fails");
            return -1;
        }

        if (xpn_compression == 1) net_t1 = std::chrono::high_resolution_clock::now();
        
        int ret_write;
        if (compressed_data_size > 0) {
            ret_write = m_comm->write_data(compressed_data, compressed_data_size);
        } else {
            ret_write = m_comm->write_data(uncompressed_buffer + (uncompressed_size - remaining), chunk_size);
        }

        if (ret_write < 0 || m_comm->read_data(&req, sizeof(req)) < 0) {
            debug_error("[SERV_ID=" << m_server << "] [NFI_XPN] [nfi_xpn_server_write] ERROR: comm error");
            return -1;
        }
        if (xpn_compression == 1) net_t2 = std::chrono::high_resolution_clock::now();

        if (xpn_compression == 1) {
            if (compressed_data_size > 0) {
                m_write_compressor.update_metrics_comp(chunk_size, compressed_data_size,
                    std::chrono::duration_cast<std::chrono::microseconds>(net_t2 - net_t1), std::chrono::microseconds(req.rw_time_us),
                    std::chrono::duration_cast<std::chrono::microseconds>(com_t2 - com_t1), std::chrono::microseconds(req.compress_time_us));
            } else {
                m_write_compressor.update_metrics(chunk_size, 
                    std::chrono::duration_cast<std::chrono::microseconds>(net_t2 - net_t1), std::chrono::microseconds(req.rw_time_us));
            }
        }

        if (req.size < 0) {
            debug_error("[SERV_ID=" << m_server << "] [NFI_XPN] [nfi_xpn_server_write] ERROR: server returned error size");
            if (req.status.ret < 0) errno = req.status.server_errno;
            return -1;
        }

        total_written += req.size;
        remaining -= chunk_size;
        current_offset += chunk_size;

    } while (remaining > 0);

    return total_written;
}

int64_t nfi_xpn_server::nfi_write_v2(std::string_view path, const xpn_fh &fh, const char *uncompressed_buffer, int64_t offset, uint64_t uncompressed_size)
{
    if (uncompressed_size == 0) return 0;

    char xpn_compression = xpn_env::get_instance().xpn_compression;
    uint64_t remaining = uncompressed_size;
    int64_t current_offset = offset;
    int64_t total_written = 0;
    bool must_compress = m_write_compressor.should_compress(uncompressed_size);

    debug_info("[SERV_ID=" << m_server << "] [NFI_XPN] [nfi_xpn_server_write] >> Begin V2");

    do {
        uint64_t chunk_size = must_compress ? std::min(remaining, (uint64_t)MAX_BUFFER_SIZE) : remaining;
        
        xpn_server_msg message;
        st_xpn_server_write_v2 *msg = reinterpret_cast<st_xpn_server_write_v2*>(message.msg_buffer);
        st_xpn_server_write_v2_req req{};
        char compressed_data[LZ4_COMPRESSBOUND(MAX_BUFFER_SIZE)];
        int compressed_data_size = 0;

        std::chrono::time_point<std::chrono::high_resolution_clock> com_t1, com_t2, net_t1, net_t2;

        if (must_compress) {
            if (xpn_compression == 1) com_t1 = std::chrono::high_resolution_clock::now();
            compressed_data_size = LZ4_compress_fast(uncompressed_buffer + (uncompressed_size - remaining), 
                                                    compressed_data, chunk_size, sizeof(compressed_data), 10);
            if (compressed_data_size < 0) return -1;
            if (xpn_compression == 1) com_t2 = std::chrono::high_resolution_clock::now();
        }

        uint32_t length = concatenate_path(msg->buff.path(), m_path, path);
        msg->buff.size_path = length;
        msg->offset = current_offset;
        msg->uncompressed_size = chunk_size;
        msg->compressed_size = compressed_data_size;
        msg->fd = fh.as.file.fd;
        msg->xpn_session = xpn_env::get_instance().xpn_session_file;
        msg->xpn_compression = xpn_compression;
        msg->buff.size_buff = (compressed_data_size > 0) ? compressed_data_size : chunk_size;

        message.op = static_cast<int>(xpn_server_ops::WRITE_FILE_V2);
        message.msg_size = msg->get_size_without_buff();
        message.tag = (int)(pthread_self() % 32450) + 1;

        struct iovec iovs[2] = {
            { .iov_base = &message, .iov_len = message.get_size() },
            { .iov_base = (compressed_data_size > 0) ? (void*)compressed_data : (void*)(uncompressed_buffer + (uncompressed_size - remaining)), 
              .iov_len = (compressed_data_size > 0) ? (size_t)compressed_data_size : (size_t)chunk_size }
        };

        if (xpn_compression == 1) net_t1 = std::chrono::high_resolution_clock::now();
        if (m_comm->writev_data(iovs, 2, 0) < 0) return -1;
        if (m_comm->read_data(&req, sizeof(req)) < 0) return -1;
        if (xpn_compression == 1) net_t2 = std::chrono::high_resolution_clock::now();

        if (xpn_compression == 1) {
            if (compressed_data_size > 0) {
                m_write_compressor.update_metrics_comp(chunk_size, compressed_data_size,
                    std::chrono::duration_cast<std::chrono::microseconds>(net_t2 - net_t1), std::chrono::microseconds(req.write_time_us),
                    std::chrono::duration_cast<std::chrono::microseconds>(com_t2 - com_t1), std::chrono::microseconds(req.decompress_time_us));
            } else {
                m_write_compressor.update_metrics(chunk_size, std::chrono::duration_cast<std::chrono::microseconds>(net_t2 - net_t1), std::chrono::microseconds(req.write_time_us));
            }
        }

        if (req.status.ret < 0) { 
            errno = req.status.server_errno; 
            return -1; 
        }
        
        total_written += req.size;
        remaining -= chunk_size;
        current_offset += chunk_size;

    } while (remaining > 0);

    return total_written;
}

int64_t nfi_xpn_server::nfi_write(std::string_view path, const xpn_fh &fh, const char *uncompressed_buffer, int64_t offset, uint64_t uncompressed_size)
{
    std::optional<std::unique_lock<std::mutex>> lock = std::nullopt;
    if (!xpn_env::get_instance().xpn_connect && m_comm == nullptr) {
        m_comm = m_control_comm_connectionless->connect(m_server, m_connectionless_port);
    } else if (m_comm->m_type == server_type::SCK) {
        auto sck_comm = static_cast<nfi_sck_server_comm*>(m_comm.get());
        lock = std::unique_lock<std::mutex>(sck_comm->m_mutex);
        debug_info("lock sck comm mutex");
    }

    int64_t ret;

    if (xpn_env::get_instance().xpn_rw_v2 == 0 || m_protocol_type != nfi_server::protocol_t::fabric) {
        ret = nfi_write_v1(path, fh, uncompressed_buffer, offset, uncompressed_size);
    } else {
        ret = nfi_write_v2(path, fh, uncompressed_buffer, offset, uncompressed_size);
    }

    if (!xpn_env::get_instance().xpn_connect) {
        m_control_comm_connectionless->disconnect(m_comm);
        m_comm = nullptr;
    }

    debug_info("[SERV_ID=" << m_server << "] [NFI_XPN] [nfi_xpn_server_write] >> End");
    return ret;
}

int nfi_xpn_server::nfi_remove (std::string_view path, bool is_async)
{
  int ret;
  st_xpn_server_path msg{};
  st_xpn_server_status req{};

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_remove] >> Begin");

  uint32_t length = concatenate_path(msg.path.path, m_path, path);
  msg.path.size = length;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_remove] nfi_xpn_server_remove("<<msg.path.path<<", "<<is_async<<")");

  if (is_async)
  {
    ret = nfi_write_operation(xpn_server_ops::RM_FILE_ASYNC, msg, false);
  }
  else
  {
    ret = nfi_do_request(xpn_server_ops::RM_FILE, msg, req);
    if (req.ret < 0)
      errno = req.server_errno;
    ret = req.ret;
  }

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_remove] nfi_xpn_server_remove("<<msg.path.path<<", "<<is_async<<")="<<ret);
  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_remove] >> End");

  return ret;
}

int nfi_xpn_server::nfi_rename (std::string_view path, std::string_view new_path)
{
  int ret;
  st_xpn_server_rename msg{};
  st_xpn_server_status req{};

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_rename] >> Begin");

  uint32_t length = concatenate_path(msg.paths.path1(), m_path, path);
  msg.paths.size1 = length;
  length = concatenate_path(msg.paths.path2(), m_path, new_path);
  msg.paths.size2 = length;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_rename] nfi_xpn_server_rename("<<msg.paths.path1()<<", "<<msg.paths.path2()<<")");

  ret = nfi_do_request(xpn_server_ops::RENAME_FILE, msg, req);
  if (req.ret < 0){
    errno = req.server_errno;
    ret = req.ret;
  }

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_rename] nfi_xpn_server_rename("<<msg.paths.path1()<<", "<<msg.paths.path2()<<")="<<ret);
  debug_info("[NFI_XPN] [nfi_xpn_server_rename] >> End");

  return ret;
}

int nfi_xpn_server::nfi_getattr (std::string_view path, struct ::stat &st)
{
  int ret;
  st_xpn_server_path msg{};
  st_xpn_server_attr_req req{};

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_getattr] >> Begin");

  uint32_t length = concatenate_path(msg.path.path, m_path, path);
  msg.path.size = length;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_getattr] nfi_xpn_server_getattr("<<msg.path.path<<")");

  ret = nfi_do_request(xpn_server_ops::GETATTR_FILE, msg, req);

  st = req.attr.to_stat();

  if (req.status_req.ret < 0){
    errno = req.status_req.server_errno;
    ret = req.status_req.ret;
  }
  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_getattr] nfi_xpn_server_getattr("<<msg.path.path<<")="<<ret);

  debug_info("[NFI_XPN] [nfi_xpn_server_getattr] >> End");

  return ret;
}

int nfi_xpn_server::nfi_setattr ([[maybe_unused]] std::string_view path, [[maybe_unused]] struct ::stat &st)
{
  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_setattr] >> Begin");

  // TODO: setattr

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_setattr] >> End");

  return 0;
}

// Directories API
int nfi_xpn_server::nfi_mkdir(std::string_view path, mode_t mode)
{
  int ret;
  st_xpn_server_path_flags msg{};
  st_xpn_server_status req{};

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_mkdir] >> Begin");

  uint32_t length = concatenate_path(msg.path.path, m_path, path);
  msg.path.size = length;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_mkdir] nfi_xpn_server_mkdir("<<msg.path.path<<")");

  msg.mode = mode;

  ret = nfi_do_request(xpn_server_ops::MKDIR_DIR, msg, req);

  if (req.ret < 0){
    errno = req.server_errno;
  }

  if ((req.ret < 0)&&(errno != EEXIST))
  {
    debug_error("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_mkdir] ERROR: xpn_mkdir fails to mkdir '"<<msg.path.path<<"'");
    return -1;
  }

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_mkdir] nfi_xpn_server_mkdir("<<msg.path.path<<")="<<ret);

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_mkdir] >> End");

  return ret;
}

int nfi_xpn_server::nfi_opendir(std::string_view path, xpn_fh &fho)
{
  int ret;
  st_xpn_server_path_flags msg{};
  st_xpn_server_opendir_req req{};

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_opendir] >> Begin");

  uint32_t length = concatenate_path(msg.path.path, m_path, path);
  msg.path.size = length;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_opendir] nfi_xpn_server_opendir("<<msg.path.path<<")");
  
  msg.xpn_session = xpn_env::get_instance().xpn_session_dir;

  ret = nfi_do_request(xpn_server_ops::OPENDIR_DIR, msg, req);
  if (req.status.ret < 0)
  {
    errno = req.status.server_errno;
    return req.status.ret;
  }

  fho.type = xpn_fh::type_t::Dir;
  fho.as.dir.dir = req.dir;
  fho.as.dir.telldir = req.status.ret;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_opendir] nfi_xpn_server_opendir("<<msg.path.path<<")="<<ret);

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_opendir] >> End");

  return ret;
}

int nfi_xpn_server::nfi_readdir(std::string_view path, xpn_fh &fhd, struct ::dirent &entry)
{
  int ret;
  st_xpn_server_readdir msg{};
  st_xpn_server_readdir_req req{};

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_readdir] >> Begin");

  // clean all entry content
  memset(&entry, 0, sizeof(struct ::dirent));

  uint32_t length = concatenate_path(msg.path.path, m_path, path);
  msg.path.size = length;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_readdir] nfi_xpn_server_readdir("<<msg.path.path<<")");

  msg.telldir =     fhd.as.dir.telldir;
  msg.dir =         fhd.as.dir.dir;
  msg.xpn_session = xpn_env::get_instance().xpn_session_dir;

  ret = nfi_do_request(xpn_server_ops::READDIR_DIR, msg, req);
  
  if (req.status.ret < 0){
    errno = req.status.server_errno;
    ret = req.status.ret;
  }else{
    fhd.as.dir.telldir = req.telldir;
  }
  
  if (req.end == 0) {
    return -1;
  }

  entry = req.ret.to_dirent();

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_readdir] nfi_xpn_server_readdir("<<msg.path.path<<")="<<(void*)&entry);
  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_readdir] >> End");

  return ret;
}

int nfi_xpn_server::nfi_closedir ([[maybe_unused]] std::string_view path, const xpn_fh &fhd)
{
  if (xpn_env::get_instance().xpn_session_dir == 1){
    int ret;
    st_xpn_server_close msg{};
    st_xpn_server_status req{};

    debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_closedir] >> Begin");

    debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_closedir] nfi_xpn_server_closedir("<<fhd.as.dir.dir<<")");

    msg.dir = fhd.as.dir.dir;

    ret = nfi_do_request(xpn_server_ops::CLOSEDIR_DIR, msg, req);

    if (req.ret < 0){
      errno = req.server_errno;
      ret = req.ret;
    }

    debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_closedir] nfi_xpn_server_closedir("<<fhd.as.dir.dir<<")="<<ret);
    debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_closedir] >> End");

    return ret;
  }else{
    // Without sesion close do nothing
    return 0;
  }
}

int nfi_xpn_server::nfi_rmdir(std::string_view path, bool is_async)
{
  int ret;
  st_xpn_server_path msg{};
  st_xpn_server_status req{};

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_rmdir] >> Begin");

  uint32_t length = concatenate_path(msg.path.path, m_path, path);
  msg.path.size = length;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_rmdir] nfi_xpn_server_rmdir("<<msg.path.path<<")");

  if (is_async)
  {
    ret = nfi_write_operation(xpn_server_ops::RMDIR_DIR_ASYNC, msg, false);
  }
  else
  {
    ret = nfi_do_request(xpn_server_ops::RMDIR_DIR, msg, req);
    if (req.ret < 0){
      errno = req.server_errno;
      ret = req.ret;
    }
  }

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_rmdir] nfi_xpn_server_rmdir("<<msg.path.path<<")="<<ret);
  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_rmdir] >> End");

  return ret;
}

int nfi_xpn_server::nfi_statvfs(std::string_view path, struct ::statvfs &inf)
{
  int ret;
  st_xpn_server_path msg{};
  st_xpn_server_statvfs_req req{};

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_statvfs] >> Begin");

  uint32_t length = concatenate_path(msg.path.path, m_path, path);
  msg.path.size = length;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_statvfs] nfi_xpn_server_statvfs("<<msg.path.path<<")");

  ret = nfi_do_request(xpn_server_ops::STATVFS_DIR, msg, req);

  inf = req.attr.to_statvfs();

  if (req.status_req.ret < 0){
    errno = req.status_req.server_errno;
    ret = req.status_req.ret;
  }

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_statvfs] nfi_xpn_server_statvfs("<<msg.path.path<<")="<<ret);
  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_statvfs] >> End");

  return ret;
}

int nfi_xpn_server::nfi_read_mdata (std::string_view path, xpn_metadata &mdata)
{
  int ret;
  st_xpn_server_path msg{};
  st_xpn_server_read_mdata_req req{};

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_read_mdata] >> Begin");

  uint32_t length = concatenate_path(msg.path.path, m_path, path);
  msg.path.size = length;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_read_mdata] nfi_xpn_server_read_mdata("<<msg.path.path<<")");

  ret = nfi_do_request(xpn_server_ops::READ_MDATA, msg, req);

  if (req.status.ret < 0){
    errno = req.status.server_errno;
    ret = req.status.ret;
  }
  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_read_mdata] nfi_xpn_server_read_mdata("<<msg.path.path<<")="<<ret);

  memcpy(&mdata.m_data, &req.mdata, sizeof(req.mdata));

  debug_info("[NFI_XPN] [nfi_xpn_server_read_mdata] >> End");

  return ret;
}

int nfi_xpn_server::nfi_write_mdata (std::string_view path, const xpn_fh &fh, const xpn_metadata::data &mdata, bool only_file_size)
{
  int ret;
  st_xpn_server_status req{};

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_write_mdata] >> Begin");

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_write_mdata] nfi_xpn_server_write_mdata("<<m_path<<"/"<<path<<")");

  if (only_file_size){
    st_xpn_server_write_mdata_file_size msg{};
    uint32_t length = concatenate_path(msg.path.path, m_path, path);
    msg.path.size = length;
    msg.size = mdata.file_size;
    msg.fd = fh.as.file.fd;
    msg.xpn_session = xpn_env::get_instance().xpn_session_file;
    // ret = nfi_do_request(xpn_server_ops::WRITE_MDATA_FILE_SIZE, msg, req);
    ret = nfi_write_operation(xpn_server_ops::WRITE_MDATA_FILE_SIZE, msg, false);
  }else{
    st_xpn_server_write_mdata msg{};
    uint32_t length = concatenate_path(msg.path.path, m_path, path);
    msg.path.size = length;
    memcpy(&msg.mdata, &mdata, sizeof(mdata));
    ret = nfi_do_request(xpn_server_ops::WRITE_MDATA, msg, req);
  }
  
  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_write_mdata] nfi_xpn_server_write_mdata ret "<<req.ret<<" server_errno "<<req.server_errno);
  if (req.ret < 0){
    errno = req.server_errno;
    ret = req.ret;
  }
  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_xpn_server_write_mdata] nfi_xpn_server_write_mdata("<<m_path<<"/"<<path<<")="<<ret);

  debug_info("[NFI_XPN] [nfi_xpn_server_write_mdata] >> End");

  return ret;
}

int nfi_xpn_server::nfi_flush (const char *path)
{
  int ret;
  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_flush] >> Begin");
  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_flush] nfi_flush("<<m_path<<", "<<path<<")");

  st_xpn_server_flush_preload_ckpt msg{};
    
  std::size_t length = m_path.copy(msg.paths.path1(), m_path.size());
  msg.paths.path1()[length] = '\0';
  msg.paths.size1 = length + 1;
  
  length = strlen(path);
  strcpy(msg.paths.path2(), path);
  msg.paths.path2()[length] = '\0';
  msg.paths.size2 = length + 1;

  ret = nfi_write_operation(xpn_server_ops::FLUSH, msg);

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_flush] nfi_flush("<<m_path<<", "<<path<<")="<<ret);
  debug_info("[NFI_XPN] [nfi_flush] >> End");
  return ret;
}

int nfi_xpn_server::nfi_preload (const char *path)
{
  int ret;
  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_preload] >> Begin");
  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_preload] nfi_preload("<<m_path<<", "<<path<<")");

  st_xpn_server_flush_preload_ckpt msg{};
  
  size_t length = strlen(path);
  strcpy(msg.paths.path1(), path);
  msg.paths.path1()[length] = '\0';
  msg.paths.size1 = length + 1;
  
  length = m_path.copy(msg.paths.path2(), m_path.size());
  msg.paths.path2()[length] = '\0';
  msg.paths.size2 = length + 1;

  ret = nfi_write_operation(xpn_server_ops::PRELOAD, msg);

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_preload] nfi_preload("<<m_path<<", "<<path<<")="<<ret);
  debug_info("[NFI_XPN] [nfi_preload] >> End");
  return ret;
}

int nfi_xpn_server::nfi_checkpoint (const char *path)
{
  int ret;
  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_checkpoint] >> Begin");
  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_checkpoint] nfi_checkpoint("<<m_path<<", "<<path<<")");

  st_xpn_server_flush_preload_ckpt msg{};
    
  std::size_t length = m_path.copy(msg.paths.path1(), m_path.size());
  msg.paths.path1()[length] = '\0';
  msg.paths.size1 = length + 1;
  
  length = strlen(path);
  strcpy(msg.paths.path2(), path);
  msg.paths.path2()[length] = '\0';
  msg.paths.size2 = length + 1;

  ret = nfi_write_operation(xpn_server_ops::CHECKPOINT, msg);

  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_checkpoint] nfi_checkpoint("<<m_path<<", "<<path<<")="<<ret);
  debug_info("[NFI_XPN] [nfi_checkpoint] >> End");
  return ret;
}

int nfi_xpn_server::nfi_response() {
  
  int ret;
  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_response] >> Begin");
  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_response] nfi_response()");

  st_xpn_server_status req{};
    
  ret = nfi_read_response(req);

  if (ret > 0) {
    if (req.ret < 0) {
      errno = req.server_errno;
      ret = req.ret;
    }
  }
  debug_info("[SERV_ID="<<m_server<<"] [NFI_XPN] [nfi_response] nfi_response()="<<ret);
  debug_info("[NFI_XPN] [nfi_response] >> End");
  return ret;
}

} // namespace XPN
