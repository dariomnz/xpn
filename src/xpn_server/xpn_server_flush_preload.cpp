
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
#include <linux/limits.h>
#include <stddef.h>

#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "base_cpp/filesystem.hpp"
#include "base_cpp/fixed_task_queue.hpp"
#include "base_cpp/timer.hpp"
#include "base_cpp/workers.hpp"
#include "xpn/xpn_file.hpp"
#include "xpn/xpn_metadata.hpp"
#include "xpn/xpn_partition.hpp"
#include "xpn_server.hpp"
#include "xpn_server/xpn_server_ops.hpp"

#ifdef ENABLE_FABRIC_SERVER
#include "lfi.h"
#include "lfi_coll.h"
#include "lfi_error.h"
#endif

namespace XPN {

struct FlushEntry {
    char src_path[PATH_MAX];
    char dest_path[PATH_MAX];
    mode_t mode;
    xpn_metadata::data mdata;
};

struct PreloadEntry {
    char src_path[PATH_MAX];
    char dest_path[PATH_MAX];
    mode_t mode;
    int64_t file_size;
};

#define CHECK_LFI(func)                                                                                 \
    do {                                                                                                \
        auto ___check_lfi = func;                                                                       \
        if (___check_lfi != LFI_SUCCESS) {                                                              \
            print("Error running " << #func << " in " << ::XPN::file_name(__FILE__) << ":" << __LINE__  \
                                   << " error: " << ___check_lfi << " " << lfi_strerror(___check_lfi)); \
        }                                                                                               \
    } while (0);

#ifdef ENABLE_FABRIC_SERVER

WorkerResult flush_file_blocks_task(std::unique_ptr<FlushEntry> entry, xpn_partition &dummy_part, int rank,
                                    std::atomic<int64_t> &g_size_copied) {
    debug_info(entry->src_path << " " << entry->dest_path << " " << entry->mdata.file_size << " "
                               << format_open_mode(entry->mode) << " " << rank << " "
                               << " " << g_size_copied.load());
    int fd_src = open64(entry->src_path, O_RDONLY | O_LARGEFILE);
    if (fd_src < 0) return WorkerResult(-1);

    int fd_dest = open64(entry->dest_path, O_WRONLY | O_CREAT | O_LARGEFILE, entry->mode);
    if (fd_dest < 0) {
        close(fd_src);
        return WorkerResult(-1);
    }

    if (rank == 0) {
        print("Size: " << format_bytes(entry->mdata.file_size) << " " << entry->src_path << " -> " << entry->dest_path);
    }

    xpn_file file("", dummy_part);
    file.m_mdata.m_data = entry->mdata;

    const int64_t file_size = entry->mdata.file_size;
    const uint64_t block_size = entry->mdata.block_size;

    int64_t bytes_copied_local = 0;
    int serv;
    int64_t off_src;

    for (int64_t off_dst = 0; off_dst < file_size; off_dst += block_size) {
        file.map_offset_mdata(off_dst, 0, off_src, serv);
        if (serv == rank) {
            off64_t src_pos = off_src + xpn_metadata::HEADER_SIZE;
            if (lseek64(fd_dest, off_dst, SEEK_SET) >= 0) {
                int64_t bytes_to_send = std::min(block_size, (uint64_t)(file_size - off_dst));
                ssize_t sent = filesystem::sendfile(fd_dest, fd_src, &src_pos, bytes_to_send);
                if (sent > 0)
                    bytes_copied_local += sent;
                else if (sent < 0) {
                    perror("sendfile flush");
                }
            } else {
                perror("lseek64 flush");
                close(fd_src);
                close(fd_dest);
                return WorkerResult(-1);
            }
        }
    }

    close(fd_src);
    close(fd_dest);

    g_size_copied += bytes_copied_local;
    return WorkerResult(0);
}

template <typename Handler, size_t Capacity>
void flush_file(lfi_group *group, FixedTaskQueue<Handler, Capacity> &tasks, std::unique_ptr<FlushEntry> entry_ptr,
                xpn_partition &dummy_part, int xpn_base_path_len, int rank, std::atomic<int64_t> &g_size_copied) {
    debug_info(entry_ptr->src_path << " " << entry_ptr->dest_path << " " << entry_ptr->mdata.file_size << " "
                                   << format_open_mode(entry_ptr->mode) << " " << rank << " "
                                   << " " << g_size_copied.load());
    std::string rel_path = &entry_ptr->src_path[xpn_base_path_len];
    xpn_file file(rel_path, dummy_part);
    int master_node = file.m_mdata.master_file();

    if (rank == master_node) {
        int fd = open(entry_ptr->src_path, O_RDONLY);
        if (fd >= 0) {
            if (read(fd, &entry_ptr->mdata, sizeof(entry_ptr->mdata)) != sizeof(entry_ptr->mdata)) {
                entry_ptr->mdata = {};
            }
            close(fd);
        } else {
            perror("open flush");
        }
    }
    CHECK_LFI(lfi_broadcast(group, master_node, &entry_ptr->mdata, sizeof(entry_ptr->mdata)));

    if (entry_ptr->mdata.is_valid() && file.exist_in_serv(rank)) {
        tasks.launch([entry = std::move(entry_ptr), &dummy_part, rank, &g_size_copied]() mutable {
            return flush_file_blocks_task(std::move(entry), dummy_part, rank, g_size_copied);
        });
    } else {
        printf("File have not valid mdata %s\n", entry_ptr->src_path);
    }
}

void flush(lfi_group *group, int xpn_base_path_len, const char *src_root, const char *dest_root, int rank,
           workers &pool, xpn_partition &dummy_part, std::atomic<int64_t> &g_size_copied) {
    debug_info(src_root << " " << dest_root << " " << rank << " "
                        << " " << g_size_copied.load());
    auto result_handler = []([[maybe_unused]] const WorkerResult &r) { return true; };
    auto tasks = FixedTaskQueueFactory<1024>::Create(pool, result_handler);

    if (rank == 0) {
        std::vector<std::pair<std::string, std::string>> stack;
        stack.push_back({src_root, dest_root});

        while (!stack.empty()) {
            auto current = stack.back();
            stack.pop_back();

            DIR *dir = opendir(current.first.c_str());
            if (!dir) continue;

            struct dirent *de;
            struct stat st;
            while ((de = readdir(dir)) != NULL) {
                if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;

                auto entry_ptr = std::make_unique<FlushEntry>();
                snprintf(entry_ptr->src_path, PATH_MAX, "%s/%s", current.first.c_str(), de->d_name);
                snprintf(entry_ptr->dest_path, PATH_MAX, "%s/%s", current.second.c_str(), de->d_name);

                if (stat(entry_ptr->src_path, &st) == 0) {
                    entry_ptr->mode = st.st_mode;
                    if (S_ISDIR(st.st_mode)) {
                        mkdir(entry_ptr->dest_path, entry_ptr->mode);
                        stack.push_back({entry_ptr->src_path, entry_ptr->dest_path});
                    } else {
                        int signal = 1;  // File signal
                        CHECK_LFI(lfi_broadcast(group, 0, &signal, sizeof(int)));
                        CHECK_LFI(lfi_broadcast(group, 0, entry_ptr.get(), sizeof(FlushEntry)));

                        flush_file(group, tasks, std::move(entry_ptr), dummy_part, xpn_base_path_len, rank,
                                   g_size_copied);
                    }
                }
            }
            closedir(dir);
        }
        int signal = 0;
        CHECK_LFI(lfi_broadcast(group, 0, &signal, sizeof(int)));
    } else {
        while (true) {
            int signal;
            CHECK_LFI(lfi_broadcast(group, 0, &signal, sizeof(int)));
            if (signal == 0) break;

            auto entry_ptr = std::make_unique<FlushEntry>();
            CHECK_LFI(lfi_broadcast(group, 0, entry_ptr.get(), sizeof(FlushEntry)));

            flush_file(group, tasks, std::move(entry_ptr), dummy_part, xpn_base_path_len, rank, g_size_copied);
        }
    }

    tasks.wait_remaining();
}
#endif

void xpn_server::op_flush(xpn_server_comm &comm, [[maybe_unused]] const st_xpn_server_flush_preload_ckpt &head,
                          int rank_client_id, int tag_client_id) {
    XPN_PROFILE_FUNCTION();
    struct st_xpn_server_status status = {};

    debug_info("[Server=" << serv_name << "] [XPN_SERVER_OPS] [op_flush] >> Begin");

    // TODO: only support fabric
    if (m_params.srv_type != server_type::FABRIC) {
        debug_info("[Server=" << serv_name << "] [XPN_SERVER_OPS] [op_flush] << End server is not fabric type");
        status.ret = -1;
        comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);
        return;
    }

#ifdef ENABLE_FABRIC_SERVER
    int ret;
    debug_info("[Server=" << serv_name << "] [XPN_SERVER_OPS] [op_flush] flush(" << head.paths.path1() << ", "
                          << head.paths.path2() << ")");

    xpn_conf conf;
    // TODO: do for more than the first partition
    std::vector<std::string> hostnames;
    std::vector<const char *> hostnames_c_str;
    for (auto &&srv_url : conf.partitions[0].server_urls) {
        xpn_url url = xpn_parser::parse(srv_url);
        hostnames.emplace_back(url.server);
    }

    for (auto &&hostname : hostnames) {
        hostnames_c_str.emplace_back(hostname.c_str());
    }
    lfi_group group = {};
    ret = lfi_group_create(hostnames_c_str.data(), hostnames_c_str.size(), &group);
    if (ret < 0) {
        debug_info("[Server=" << serv_name << "] [XPN_SERVER_OPS] [op_flush] << End lfi error " << lfi_strerror(ret));
        status.ret = ret;
        comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);
        return;
    }

    int rank, size;
    CHECK_LFI(lfi_group_rank(&group, &rank));
    CHECK_LFI(lfi_group_size(&group, &size));
    double start_time = lfi_time(&group);

    int xpn_base_path_len = strlen(head.paths.path1());
    std::atomic<int64_t> g_size_copied{0};
    xpn_partition dummy_part("xpn", conf.partitions[0].replication_level, conf.partitions[0].bsize);
    dummy_part.m_data_serv.resize(size);

    flush(&group, xpn_base_path_len, head.paths.path1(), head.paths.path2(), rank, *m_worker1, dummy_part,
          g_size_copied);

    double total_time = lfi_time(&group) - start_time;
    int64_t total_size_copied = g_size_copied.load();
    debug_info("Local data moved: " << format_bytes(total_size_copied) << " ("
                                    << format_bytes(total_size_copied / total_time) << "/s)");
    CHECK_LFI(
        lfi_allreduce(&group, &total_size_copied, lfi_op_type_enum::LFI_OP_TYPE_INT64_T, lfi_op_enum::LFI_OP_SUM));

    if (rank == 0) {
        printf("Flush completed in %.2f ms\n", total_time * 1000.0);
        print("Total data moved: " << format_bytes(total_size_copied) << " ("
                                   << format_bytes(total_size_copied / total_time) << "/s)");
    }

    CHECK_LFI(lfi_group_close(&group));

    status.ret = 0;
    comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);

    debug_info("[Server=" << serv_name << "] [XPN_SERVER_OPS] [op_flush] << End");
#endif
}
#ifdef ENABLE_FABRIC_SERVER

WorkerResult preload_file_blocks_task(std::unique_ptr<PreloadEntry> entry, xpn_partition &dummy_part,
                                      int xpn_base_path_len, int rank, int size, std::atomic<int64_t> &g_size_copied) {
    debug_info(entry->src_path << " " << entry->dest_path << " " << entry->file_size << " "
                               << format_open_mode(entry->mode) << " " << rank << " " << size << " "
                               << g_size_copied.load());

    std::string rel_path = &entry->dest_path[xpn_base_path_len];
    xpn_file file(rel_path, dummy_part);
    file.m_mdata.m_data.fill(file.m_mdata);
    file.m_mdata.m_data.file_size = entry->file_size;

    if (!file.exist_in_serv(rank)) return WorkerResult(0);

    int fd_src = open64(entry->src_path, O_RDONLY | O_LARGEFILE);
    if (fd_src < 0) return WorkerResult(-1);

    int fd_dest = open64(entry->dest_path, O_CREAT | O_WRONLY | O_TRUNC | O_LARGEFILE, entry->mode);
    if (fd_dest < 0) {
        close(fd_src);
        return WorkerResult(-1);
    }

    if (rank == 0) {
        print("Size: " << format_bytes(entry->file_size) << " " << entry->src_path << " -> " << entry->dest_path);
    }

    int64_t bytes_copied_local = 0;
    int64_t offset_src = 0;
    int64_t local_offset;
    int local_server;

    while (offset_src < static_cast<int64_t>(entry->file_size)) {
        for (int i = 0; i <= dummy_part.m_replication_level; i++) {
            file.map_offset_mdata(offset_src, i, local_offset, local_server);
            if (local_server == rank) {
                off64_t src_pos = offset_src;
                off64_t dst_pos = local_offset + xpn_metadata::HEADER_SIZE;
                int64_t to_copy = std::min(static_cast<int64_t>(dummy_part.m_block_size),
                                           static_cast<int64_t>(entry->file_size - offset_src));

                if (lseek64(fd_dest, dst_pos, SEEK_SET) >= 0) {
                    ssize_t sent = filesystem::sendfile(fd_dest, fd_src, &src_pos, to_copy);
                    if (sent > 0) bytes_copied_local += sent;
                }
            }
        }
        offset_src += (int64_t)dummy_part.m_block_size;
    }

    // Write metadata if this rank is one of the replica nodes for metadata
    bool write_mdata = false;
    for (int i = 0; i <= dummy_part.m_replication_level; i++) {
        int aux_serv = (file.m_mdata.m_data.first_node + i) % size;
        if (aux_serv == rank) {
            write_mdata = true;
            break;
        }
    }

    if (write_mdata) {
        pwrite64(fd_dest, &file.m_mdata.m_data, sizeof(file.m_mdata.m_data), 0);
    }

    close(fd_src);
    close(fd_dest);

    g_size_copied += bytes_copied_local;
    return WorkerResult(0);
}

void preload(lfi_group *group, int xpn_base_path_len, const char *src_root, const char *dest_root, int rank, int size,
             workers &pool, int blocksize, int replication_level, std::atomic<int64_t> &g_size_copied) {
    debug_info(group << " " << xpn_base_path_len << " " << src_root << " " << dest_root << " " << rank << " " << size
                     << " " << blocksize << " " << replication_level << " " << g_size_copied.load());
    auto result_handler = []([[maybe_unused]] const WorkerResult &r) { return true; };
    auto tasks = FixedTaskQueueFactory<1024>::Create(pool, result_handler);

    xpn_partition dummy_part("xpn", replication_level, blocksize);
    dummy_part.m_data_serv.resize(size);

    if (rank == 0) {
        std::vector<std::pair<std::string, std::string>> stack;
        stack.push_back({src_root, dest_root});

        while (!stack.empty()) {
            auto current = stack.back();
            stack.pop_back();

            DIR *dir = opendir(current.first.c_str());
            if (!dir) continue;

            struct dirent *de;
            struct stat st;
            while ((de = readdir(dir)) != NULL) {
                if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;

                auto entry_ptr = std::make_unique<PreloadEntry>();
                snprintf(entry_ptr->src_path, PATH_MAX, "%s/%s", current.first.c_str(), de->d_name);
                snprintf(entry_ptr->dest_path, PATH_MAX, "%s/%s", current.second.c_str(), de->d_name);

                if (stat(entry_ptr->src_path, &st) == 0) {
                    entry_ptr->mode = st.st_mode;
                    entry_ptr->file_size = st.st_size;
                    if (S_ISDIR(st.st_mode)) {
                        mkdir(entry_ptr->dest_path, entry_ptr->mode);
                        stack.push_back({entry_ptr->src_path, entry_ptr->dest_path});
                    } else {
                        int signal = 1;
                        CHECK_LFI(lfi_broadcast(group, 0, &signal, sizeof(int)));
                        CHECK_LFI(lfi_broadcast(group, 0, entry_ptr.get(), sizeof(PreloadEntry)));

                        tasks.launch([entry = std::move(entry_ptr), &dummy_part, xpn_base_path_len, rank, size,
                                      &g_size_copied]() mutable {
                            return preload_file_blocks_task(std::move(entry), dummy_part, xpn_base_path_len, rank, size,
                                                            g_size_copied);
                        });
                    }
                }
            }
            closedir(dir);
        }
        int signal = 0;
        CHECK_LFI(lfi_broadcast(group, 0, &signal, sizeof(int)));
    } else {
        while (true) {
            int signal;
            CHECK_LFI(lfi_broadcast(group, 0, &signal, sizeof(int)));
            if (signal == 0) break;

            auto entry_ptr = std::make_unique<PreloadEntry>();
            CHECK_LFI(lfi_broadcast(group, 0, entry_ptr.get(), sizeof(PreloadEntry)));

            tasks.launch(
                [entry = std::move(entry_ptr), &dummy_part, xpn_base_path_len, rank, size, &g_size_copied]() mutable {
                    return preload_file_blocks_task(std::move(entry), dummy_part, xpn_base_path_len, rank, size,
                                                    g_size_copied);
                });
        }
    }

    tasks.wait_remaining();
}
#endif

void xpn_server::op_preload(xpn_server_comm &comm, [[maybe_unused]] const st_xpn_server_flush_preload_ckpt &head,
                            int rank_client_id, int tag_client_id) {
    XPN_PROFILE_FUNCTION();
    struct st_xpn_server_status status = {};

    debug_info("[Server=" << serv_name << "] [XPN_SERVER_OPS] [op_preload] >> Begin");

    // TODO: only support fabric
    if (m_params.srv_type != server_type::FABRIC) {
        status.ret = -1;
        comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);
        return;
    }

#ifdef ENABLE_FABRIC_SERVER
    int ret;
    debug_info("[Server=" << serv_name << "] [XPN_SERVER_OPS] [op_preload] flush(" << head.paths.path1() << ", "
                          << head.paths.path2() << ")");

    xpn_conf conf;
    // TODO: do for more than the first partition
    std::vector<std::string> hostnames;
    std::vector<const char *> hostnames_c_str;
    for (auto &&srv_url : conf.partitions[0].server_urls) {
        xpn_url url = xpn_parser::parse(srv_url);
        hostnames.emplace_back(url.server);
    }

    for (auto &&hostname : hostnames) {
        hostnames_c_str.emplace_back(hostname.c_str());
    }

    lfi_group group = {};
    ret = lfi_group_create(hostnames_c_str.data(), hostnames_c_str.size(), &group);
    if (ret < 0) {
        status.ret = ret;
        comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);
        return;
    }

    int rank, size;
    CHECK_LFI(lfi_group_rank(&group, &rank));
    CHECK_LFI(lfi_group_size(&group, &size));
    double start_time = lfi_time(&group);

    int xpn_base_path_len = strlen(head.paths.path2());
    std::atomic<int64_t> g_size_copied{0};
    preload(&group, xpn_base_path_len, head.paths.path1(), head.paths.path2(), rank, size, *m_worker1,
            conf.partitions[0].bsize, conf.partitions[0].replication_level, g_size_copied);
    double total_time = lfi_time(&group) - start_time;
    int64_t total_size_copied = g_size_copied.load();
    debug_info("Local data moved: " << format_bytes(total_size_copied) << " ("
                                    << format_bytes(total_size_copied / total_time) << "/s)");
    CHECK_LFI(
        lfi_allreduce(&group, &total_size_copied, lfi_op_type_enum::LFI_OP_TYPE_INT64_T, lfi_op_enum::LFI_OP_SUM));

    if (rank == 0) {
        printf("Preload completed in %.2f ms\n", total_time * 1000.0);
        print("Total data moved: " << format_bytes(total_size_copied) << " ("
                                   << format_bytes(total_size_copied / total_time) << "/s)");
    }

    CHECK_LFI(lfi_group_close(&group));

    status.ret = 0;
    status.server_errno = 0;
    comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);

    debug_info("[Server=" << serv_name << "] [XPN_SERVER_OPS] [op_preload] << End");
#endif
}

}  // namespace XPN
