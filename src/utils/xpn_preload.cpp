
/*
 *  Copyright 2020-2025 Felix Garcia Carballeira, Diego Camarmas Alonso, Alejandro Calderon Mateos, Dario Muñoz Muñoz
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

/* ... Include / Inclusion ........................................... */

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <linux/limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <string>
#include <vector>

#include "base_cpp/debug.hpp"
#include "base_cpp/filesystem.hpp"
#include "base_cpp/fixed_task_queue.hpp"
#include "base_cpp/workers.hpp"
#include "mpi.h"
#include "xpn/xpn_file.hpp"
#include "xpn/xpn_metadata.hpp"
#include "xpn/xpn_partition.hpp"

/* ... Const / Const ................................................. */

using namespace XPN;

struct PreloadEntry {
    char src_path[PATH_MAX];
    char dest_path[PATH_MAX];
    mode_t mode;
    int64_t file_size;
};

int g_xpn_path_len = 0;
std::atomic<int64_t> g_size_copied{0};

/* ... Functions / Funciones ......................................... */

/**
 * @brief Task-based copy of file blocks.
 */
WorkerResult copy_file_blocks_task(std::unique_ptr<PreloadEntry> entry, xpn_partition& dummy_part, int rank, int size,
                                   int blocksize, int replication_level) {
    int fd_src = open64(entry->src_path, O_RDONLY | O_LARGEFILE);
    if (fd_src < 0) return WorkerResult(-1);

    int fd_dest = open64(entry->dest_path, O_CREAT | O_WRONLY | O_TRUNC | O_LARGEFILE, entry->mode);
    if (fd_dest < 0) {
        close(fd_src);
        return WorkerResult(-1);
    }

    if (rank == 0) {
        printf("Size: %ld %s -> %s\n", entry->file_size, entry->src_path, entry->dest_path);
    }

    std::string rel_path = &entry->dest_path[g_xpn_path_len];
    xpn_file file(rel_path, dummy_part);
    file.m_mdata.m_data.fill(file.m_mdata);
    file.m_mdata.m_data.file_size = entry->file_size;

    int64_t bytes_copied_local = 0;
    int64_t offset_src = 0;
    int64_t local_offset;
    int local_server;

    while (offset_src < entry->file_size) {
        for (int i = 0; i <= replication_level; i++) {
            file.map_offset_mdata(offset_src, i, local_offset, local_server);
            if (local_server == rank) {
                off64_t src_pos = offset_src;
                off64_t dst_pos = local_offset + xpn_metadata::HEADER_SIZE;
                int64_t to_copy = std::min((int64_t)blocksize, entry->file_size - offset_src);

                if (lseek64(fd_dest, dst_pos, SEEK_SET) >= 0) {
                    ssize_t sent = filesystem::sendfile(fd_dest, fd_src, &src_pos, to_copy);
                    if (sent > 0) bytes_copied_local += sent;
                }
            }
        }
        offset_src += (int64_t)blocksize;
    }

    // Write metadata if this rank is one of the replica nodes for metadata
    bool write_mdata = false;
    for (int i = 0; i <= replication_level; i++) {
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

/**
 * @brief Scan the source directory and distribute work via MPI_Bcast.
 */
void preload(const char* src_root, const char* dest_root, int rank, int size, workers& pool, int blocksize,
             int replication_level) {
    auto result_handler = []([[maybe_unused]] const WorkerResult& r) { return true; };
    auto tasks = FixedTaskQueueFactory<1024>::Create(pool, result_handler);

    xpn_partition dummy_part("xpn", replication_level, blocksize);
    dummy_part.m_data_serv.resize(size);

    if (rank == 0) {
        std::vector<std::pair<std::string, std::string>> stack;
        stack.push_back({src_root, dest_root});

        while (!stack.empty()) {
            auto current = stack.back();
            stack.pop_back();

            DIR* dir = opendir(current.first.c_str());
            if (!dir) continue;

            struct dirent* de;
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
                        printf("%s -> %s\n", entry_ptr->src_path, entry_ptr->dest_path);

                        int signal = 1;
                        MPI_Bcast(&signal, 1, MPI_INT, 0, MPI_COMM_WORLD);
                        MPI_Bcast(entry_ptr.get(), sizeof(PreloadEntry), MPI_CHAR, 0, MPI_COMM_WORLD);

                        tasks.launch([entry = std::move(entry_ptr), &dummy_part, rank, size, blocksize,
                                      replication_level]() mutable {
                            return copy_file_blocks_task(std::move(entry), dummy_part, rank, size, blocksize,
                                                         replication_level);
                        });
                    }
                }
            }
            closedir(dir);
        }
        int signal = 0;
        MPI_Bcast(&signal, 1, MPI_INT, 0, MPI_COMM_WORLD);
    } else {
        while (true) {
            int signal;
            MPI_Bcast(&signal, 1, MPI_INT, 0, MPI_COMM_WORLD);
            if (signal == 0) break;

            auto entry_ptr = std::make_unique<PreloadEntry>();
            MPI_Bcast(entry_ptr.get(), sizeof(PreloadEntry), MPI_CHAR, 0, MPI_COMM_WORLD);

            tasks.launch([entry = std::move(entry_ptr), &dummy_part, rank, size, blocksize,
                          replication_level]() mutable {
                return copy_file_blocks_task(std::move(entry), dummy_part, rank, size, blocksize, replication_level);
            });
        }
    }

    tasks.wait_remaining();
}

int main(int argc, char* argv[]) {
    int rank, size;
    int replication_level = 0;
    int blocksize = 524288;
    double start_time;

    if (argc < 3) {
        printf("Usage:\n");
        printf(" ./%s <origin local path> <destination partition> [blocksize] [replication]\n", argv[0]);
        return -1;
    }

    if (argc >= 5) replication_level = atoi(argv[4]);
    if (argc >= 4) blocksize = atoi(argv[3]);

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    start_time = MPI_Wtime();

    if (rank == 0) {
        printf("Preloading from %s to %s (BlockSize: %d, Replication: %d)\n", argv[1], argv[2], blocksize,
               replication_level);
        mkdir(argv[2], 0755);
    }

    g_xpn_path_len = strlen(argv[2]);

    auto pool = workers::Create(workers_mode::thread_pool);

    preload(argv[1], argv[2], rank, size, *pool, blocksize, replication_level);

    MPI_Barrier(MPI_COMM_WORLD);
    int64_t total_size_copied = 0;
    int64_t local_size = g_size_copied.load();
    MPI_Reduce(&local_size, &total_size_copied, 1, MPI_INT64_T, MPI_SUM, 0, MPI_COMM_WORLD);

    double total_time = MPI_Wtime() - start_time;
    if (rank == 0) {
        printf("Preload completed in %.2f ms\n", total_time * 1000.0);
        print("Total data moved: " << format_bytes(total_size_copied) << " ("
                                   << format_bytes(total_size_copied / total_time) << "/s)");
    }

    MPI_Finalize();
    return 0;
}
