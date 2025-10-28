
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
// #define DEBUG
#include <fcntl.h>
#include <sys/sendfile.h>

#include "xpn_server.hpp"

namespace XPN {

const char* s_serv_name = "nullptr";
std::chrono::time_point<std::chrono::high_resolution_clock> s_start_time;
static constexpr int BUFFER_SIZE = 1024 * 1024;

int checkpoint_dir(std::unique_ptr<xpn_server_filesystem>& fs, const std::string& to_path, uint32_t mode) {
    XPN_PROFILE_FUNCTION();
    debug_info("[Server=" << s_serv_name << "] [XPN_SERVER_OPS] [checkpoint_dir] >> Begin (" << to_path << ", "
                          << format_open_mode(mode) << ")");
    int ret = fs->mkdir(to_path.c_str(), mode);

    if (ret < 0) {
        if (errno == EEXIST) {
            debug_info("[Server=" << s_serv_name << "] [XPN_SERVER_OPS] [checkpoint_dir] << End EEXIST (" << to_path
                                  << ", " << format_open_mode(mode) << ")");
            return 0;
        }
        debug_error("mkdir " << to_path << " " << format_open_mode(mode) << " " << strerror(errno));
        return ret;
    }
    debug_info("[Server=" << s_serv_name << "] [XPN_SERVER_OPS] [checkpoint_dir] << End OK (" << to_path << ", "
                          << format_open_mode(mode) << ")");
    return 0;
}

static int checkpoint_file_chunk_task(std::unique_ptr<xpn_server_filesystem>& fs, int from_fd, int to_fd,
                                      int64_t start_offset, int64_t bytes_to_copy) {
    XPN_PROFILE_FUNCTION();
    debug_info("[Server=" << s_serv_name << "] [XPN_SERVER_OPS] [checkpoint_file_chunk_task] >> Begin (" << from_fd
                          << ", " << to_fd << ", " << start_offset << ", " << bytes_to_copy << ")");
    char buffer[BUFFER_SIZE];
    int64_t remaining_bytes = bytes_to_copy;
    int64_t current_offset = start_offset;

    while (remaining_bytes > 0) {
        ssize_t read_size = (remaining_bytes < BUFFER_SIZE) ? remaining_bytes : BUFFER_SIZE;

        ssize_t bytes_read = fs->pread(from_fd, buffer, read_size, current_offset);

        if (bytes_read <= 0) {
            debug_error("pread " << from_fd << " size " << read_size << " offset " << current_offset << " "
                                 << " " << strerror(errno));
            return bytes_read;
        }

        ssize_t bytes_written = fs->pwrite(to_fd, buffer, bytes_read, current_offset);

        if (bytes_written != bytes_read) {
            debug_error("pwrite " << to_fd << " size " << bytes_read << " offset " << current_offset << " "
                                  << " " << strerror(errno));
            return -1;
        }

        current_offset += bytes_written;
        remaining_bytes -= bytes_written;
    }

    debug_info("[Server=" << s_serv_name << "] [XPN_SERVER_OPS] [checkpoint_file_chunk_task] << End (" << from_fd
                          << ", " << to_fd << ", " << start_offset << ", " << bytes_to_copy << ")");
    return 0;
}

int checkpoint_file(std::unique_ptr<workers>& worker, std::unique_ptr<xpn_server_filesystem>& fs,
                    const std::string& from_path, const std::string& to_path, uint32_t mode) {
    XPN_PROFILE_FUNCTION();
    debug_info("[Server=" << s_serv_name << "] [XPN_SERVER_OPS] [checkpoint_file] >> Begin (" << from_path << ", "
                          << to_path << ", " << format_open_mode(mode) << ")");
#ifdef DEBUG
    timer t;
#endif
    int err = 0;
    int64_t file_size;
    struct stat from_stat;
    struct stat to_stat;

    if (fs->stat(from_path.c_str(), &from_stat) == -1) {
        debug_error("stat " << from_path << " " << strerror(errno));
        return -1;
    }

    // Check if checkpoint is necesary
    if (fs->stat(to_path.c_str(), &to_stat) != -1) {
        // Check with the existing checkpoint file
        std::time_t from_mtime_t = from_stat.st_mtime;
        std::time_t to_mtime_t = to_stat.st_mtime;
        if (from_mtime_t < to_mtime_t) {
            debug_info("[Server=" << s_serv_name
                                  << "] [XPN_SERVER_OPS] [checkpoint_file] << End older than existing ckpt file "
                                  << t.elapsedMilli() << " ms (" << from_path << ", " << to_path << ", "
                                  << format_open_mode(mode) << ")");
            return 0;
        }
    } else {
        // Check with the server start
        std::time_t from_mtime_t = from_stat.st_mtime;
        std::time_t srv_mtime_t = std::chrono::high_resolution_clock::to_time_t(s_start_time);
        if (from_mtime_t < srv_mtime_t) {
            debug_info("[Server=" << s_serv_name
                                  << "] [XPN_SERVER_OPS] [checkpoint_file] << End older than start server "
                                  << t.elapsedMilli() << " ms (" << from_path << ", " << to_path << ", "
                                  << format_open_mode(mode) << ")");
            return 0;
        }
    }

    file_size = from_stat.st_size;

    int from_fd = fs->open(from_path.c_str(), O_RDONLY);
    if (from_fd == -1) {
        debug_error("open " << from_path << " O_RDONLY " << strerror(errno));
        return -1;
    }

    int to_fd = fs->open(to_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, mode);
    if (to_fd == -1) {
        debug_error("open " << from_path << " O_WRONLY | O_CREAT | O_TRUNC " << format_open_mode(mode) << " "
                            << strerror(errno));
        fs->close(from_fd);
        return -1;
    }

    err = 0;
    // Some test done to organize the order
    // File   | copy_file_range | sendfile | read_write
    // 5GiB   |          833 ms |  1081 ms |    2560 ms
    // 100MiB |           55 ms |    59 ms |      48 ms
    if (fs->m_mode == filesystem_mode::disk) {
        // First try with copy_file_range to use kernel optimizations
        debug_info("[Server=" << s_serv_name << "] [XPN_SERVER_OPS] [checkpoint_file] Try copy_file_range");
        err = copy_file_range(from_fd, NULL, to_fd, NULL, file_size, 0);
        if (err < 0) {
            debug_error("copy_file_range " << from_fd << " " << to_fd << " " << file_size << " " << strerror(errno));
            debug_info("[Server=" << s_serv_name << "] [XPN_SERVER_OPS] [checkpoint_file] Try sendfile");
            // Second try if copy_file_range fails to use sendfile to use other kernel optimizations
            err = sendfile(to_fd, from_fd, NULL, file_size);
            if (err < 0) {
                debug_error("sendfile " << from_fd << " " << to_fd << " " << file_size << " " << strerror(errno));
            }
        }
    }
    // If is proxy server it need to be this method to pass it to the next servers
    // also if the copy_file_range and the sendfile fails we try this
    if (fs->m_mode != filesystem_mode::disk || err < 0) {
        debug_info("[Server=" << s_serv_name << "] [XPN_SERVER_OPS] [checkpoint_file] Try read write");
        const int THREAD_PER_FILE = worker->size();
        int64_t min_threads_needed = (file_size + BUFFER_SIZE - 1) / BUFFER_SIZE;

        int effective_threads = std::min((int64_t)THREAD_PER_FILE, min_threads_needed);
        if (effective_threads < 1 && file_size > 0) {
            effective_threads = 1;
        }

        int64_t chunk_size_for_split = file_size / effective_threads;
        int64_t current_offset = 0;

        std::vector<std::future<int> > v_fut;
        v_fut.reserve(effective_threads);
        for (int i = 0; i < effective_threads; ++i) {
            int64_t bytes_to_copy;

            if (i == effective_threads - 1) {
                bytes_to_copy = file_size - current_offset;
            } else {
                bytes_to_copy = chunk_size_for_split;
            }

            if (bytes_to_copy > 0) {
                v_fut.emplace_back(worker->launch([&fs, from_fd, to_fd, current_offset, bytes_to_copy]() {
                    return checkpoint_file_chunk_task(fs, from_fd, to_fd, current_offset, bytes_to_copy);
                }));
                current_offset += bytes_to_copy;
            }
        }

        err = 0;
        for (auto& fut : v_fut) {
            if (fut.valid()) {
                int ret = fut.get();
                if (ret < 0) {
                    err -= 1;
                }
            }
        }
    }

    fs->close(from_fd);
    fs->close(to_fd);

    if (err < 0) {
        debug_info("[Server=" << s_serv_name << "] [XPN_SERVER_OPS] [checkpoint_file] << End error " << t.elapsedMilli()
                              << " ms " << err << " (" << from_path << ", " << to_path << ", " << format_open_mode(mode)
                              << ")");
        return err;
    }

    debug_info("[Server=" << s_serv_name << "] [XPN_SERVER_OPS] [checkpoint_file] << End " << t.elapsedMilli()
                          << " ms (" << from_path << ", " << to_path << ", " << format_open_mode(mode) << ")");
    return 0;
}

int checkpoint_recursive(std::unique_ptr<workers>& worker, std::unique_ptr<xpn_server_filesystem>& fs,
                         const std::string& from_path, const std::string& to_path) {
    XPN_PROFILE_FUNCTION();
    debug_info("[Server=" << s_serv_name << "] [XPN_SERVER_OPS] [checkpoint_recursive] >> Begin (" << from_path << ", "
                          << to_path << ")");
    int ret;
    struct stat path_stat;

    if (fs->stat(from_path.c_str(), &path_stat) != 0) {
        debug_error("stat " << from_path << " " << strerror(errno));
        return -1;
    }

    bool is_file;

    if (S_ISDIR(path_stat.st_mode)) {
        is_file = false;
    } else if (S_ISREG(path_stat.st_mode)) {
        is_file = true;
    } else {
        // Other type of file or dir skip
        debug_info("[Server=" << s_serv_name << "] [XPN_SERVER_OPS] [checkpoint_recursive] Skipping (" << from_path
                              << ")");
        return 0;
    }

    if (is_file) {
        ret = checkpoint_file(worker, fs, from_path, to_path, path_stat.st_mode);
        debug_info("[Server=" << s_serv_name << "] [XPN_SERVER_OPS] [checkpoint_recursive] << End file (" << from_path
                              << ", " << to_path << ")");
        return ret;
    }

    ret = checkpoint_dir(fs, to_path, path_stat.st_mode);
    if (ret < 0) {
        debug_error("checkpoint_dir " << to_path << " " << strerror(errno));
        return ret;
    }

    DIR* dir = NULL;
    std::string next_from_path;
    std::string next_to_path;

    dir = fs->opendir(from_path.c_str());
    if (dir == NULL) {
        fprintf(stderr, "opendir error %s %s\n", from_path.c_str(), strerror(errno));
        debug_error("opendir " << from_path << " " << strerror(errno));
        return -1;
    }

    struct dirent* entry;
    do {
        entry = fs->readdir(dir);
        if (entry == nullptr) break;

        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }

        next_from_path = from_path + "/" + entry->d_name;
        next_to_path = to_path + "/" + entry->d_name;

        ret = checkpoint_recursive(worker, fs, next_from_path, next_to_path);
        if (ret < 0) {
            debug_error("checkpoint_recursive(" << next_from_path << ", " << next_to_path << ") " << strerror(errno));
            return ret;
        }
    } while (entry != NULL);

    fs->closedir(dir);
    debug_info("[Server=" << s_serv_name << "] [XPN_SERVER_OPS] [checkpoint_recursive] << End dir (" << from_path
                          << ", " << to_path << ")");
    return 0;
}

void xpn_server::op_checkpoint(xpn_server_comm& comm, const st_xpn_server_flush_preload_ckpt& head, int rank_client_id,
                               int tag_client_id) {
    XPN_PROFILE_FUNCTION();
    struct st_xpn_server_status status = {};
    debug_info("[Server=" << serv_name << "] [XPN_SERVER_OPS] [op_checkpoint] >> Begin");

    debug_info("[Server=" << serv_name << "] [XPN_SERVER_OPS] [op_checkpoint] checkpoint(" << head.paths.path1() << ", "
                          << head.paths.path2() << ")");
    s_serv_name = serv_name;
    s_start_time = m_start_time;
    status.ret = checkpoint_recursive(m_worker2, m_filesystem, head.paths.path1(), head.paths.path2());
    status.server_errno = errno;
    comm.write_data((char*)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);

    debug_info("[Server=" << serv_name << "] [XPN_SERVER_OPS] [op_checkpoint] << End");
}
}  // namespace XPN
