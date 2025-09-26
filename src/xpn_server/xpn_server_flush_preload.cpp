
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
#include <stddef.h>

#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>

#include "base_cpp/filesystem.hpp"
#include "base_cpp/timer.hpp"
#include "lfi_coll.h"
#include "xpn/xpn_file.hpp"
#include "xpn/xpn_metadata.hpp"
#include "xpn/xpn_partition.hpp"
#include "xpn_server.hpp"
namespace XPN {

lfi_group group;
char src_path[PATH_MAX + 5];
char dest_path[PATH_MAX + 5];

int xpn_path_len = 0;

int flush_copy(const char *entry, int is_file, const char *dir_name, const char *dest_prefix, int blocksize,
               int replication_level, int rank, int size) {
    debug_info("BEGIN entry " << entry << " is_file " << is_file << " dir_name " << dir_name << " dest_prefix "
                              << dest_prefix << " blocksize " << blocksize << " replication_level " << replication_level
                              << " rank " << rank << " size " << size);
    int ret;

    int fd_src, fd_dest, replication = 0;
    int aux_serv;
    char *buf;
    int buf_len;
    off64_t offset_dest;
    off_t offset_src;
    ssize_t read_size, write_size;
    struct stat st_src = {};
    // Alocate buffer
    buf_len = blocksize;
    buf = (char *)malloc(buf_len);
    if (NULL == buf) {
        print_error("malloc: ");
        return -1;
    }

    // Generate source path
    strcpy(src_path, entry);

    // Generate destination path
    const char *aux_entry = entry + strlen(dir_name);
    sprintf(dest_path, "%s/%s", dest_prefix, aux_entry);

    if (rank == 0) {
        debug_info(src_path << " -> " << dest_path);
    }

    ret = stat(src_path, &st_src);
    if (ret < 0 && errno != ENOENT) {
        print_error("stat: "<<src_path);
        free(buf);
        return -1;
    }
    if (!is_file) {
        if (rank == 0) {
            ret = mkdir(dest_path, st_src.st_mode);
            if (ret < 0 && errno != EEXIST) {
                print_error("mkdir: "<<dest_path);
                free(buf);
                return -1;
            }
        }
        lfi_barrier(&group);
    } else if (is_file) {
        xpn_partition part("xpn", replication_level, blocksize);
        part.m_data_serv.resize(size);
        std::string aux_str = &src_path[xpn_path_len];
        xpn_file file(aux_str, part);
        file.m_mdata.m_data.fill(file.m_mdata);
        int master_node = file.m_mdata.master_file();
        if (rank == master_node) {
            fd_dest = creat(dest_path, st_src.st_mode);
            if (fd_dest < 0) {
                print_error("creat 1: "<<dest_path);
                printf("creat: %s mode %d\n", dest_path, st_src.st_mode);
            } else {
                close(fd_dest);
            }
        }
        lfi_broadcast(&group, master_node, &fd_dest, sizeof(fd_dest));
        if (fd_dest < 0) {
            free(buf);
            return -1;
        }
        fd_src = open64(src_path, O_RDONLY | O_LARGEFILE);
        if (fd_src < 0 && errno != ENOENT) {
            print_error("open 1: "<<src_path);
            printf("open 1: %s\n", src_path);
            free(buf);
            return -1;
        }
        lfi_barrier(&group);
        fd_dest = open64(dest_path, O_WRONLY | O_LARGEFILE);
        if (fd_dest < 0) {
            print_error("open 2: "<<dest_path);
            printf("open 2: %s\n", dest_path);
            free(buf);
            return -1;
        }

        if (rank == master_node) {
            ret = filesystem::read(fd_src, &file.m_mdata.m_data, sizeof(file.m_mdata.m_data));
            // To debug
            // XpnPrintMetadata(&mdata);
        }

        debug_info("Rank " << rank << " mdata " << file.m_mdata.m_data.magic_number[0]
                           << file.m_mdata.m_data.magic_number[1] << file.m_mdata.m_data.magic_number[2]);
        lfi_broadcast(&group, master_node, &file.m_mdata.m_data, sizeof(file.m_mdata.m_data));
        debug_info("After bcast Rank " << rank << " mdata " << file.m_mdata.m_data.magic_number[0]
                                       << file.m_mdata.m_data.magic_number[1] << file.m_mdata.m_data.magic_number[2]);
#ifdef DEBUG
        std::cerr << file.m_mdata.to_string() << std::endl;
#endif
        if (!file.m_mdata.m_data.is_valid()) {
            free(buf);
            return -1;
        }

        off64_t ret_1;
        offset_src = 0;
        offset_dest = -blocksize;

        do {
            // TODO: check when the server has error and data is corrupt for fault tolerance
            do {
                offset_dest += blocksize;
                for (int i = 0; i < replication_level + 1; i++) {
                    file.map_offset_mdata(offset_dest, i, offset_src, aux_serv);

                    debug_info("try rank " << rank << " offset_dest " << offset_dest << " offset_src " << offset_src
                                           << " aux_server " << aux_serv << " file_size "
                                           << file.m_mdata.m_data.file_size);
                    if (aux_serv == rank) {
                        goto exit_search;
                    }
                }
            } while (offset_dest < static_cast<off64_t>(file.m_mdata.m_data.file_size));
        exit_search:
            if (aux_serv != rank) {
                continue;
            }
            debug_info("rank " << rank << " offset_dest " << offset_dest << " offset_src " << offset_src
                               << " aux_server " << aux_serv);
            if (st_src.st_mtime != 0 && offset_src > st_src.st_size) {
                break;
            }
            if (offset_dest > static_cast<off64_t>(file.m_mdata.m_data.file_size)) {
                break;
            }
            if (replication != 0) {
                offset_src += blocksize;
                continue;
            }

            ret_1 = lseek64(fd_src, offset_src + xpn_metadata::HEADER_SIZE, SEEK_SET);
            if (ret_1 < 0) {
                print_error("lseek: ");
                break;
            }
            read_size = filesystem::read(fd_src, buf, buf_len);
            if (read_size <= 0) {
                break;
            }

            ret_1 = lseek64(fd_dest, offset_dest, SEEK_SET);
            if (ret_1 < 0) {
                print_error("lseek: ");
                break;
            }
            write_size = filesystem::write(fd_dest, buf, read_size);
            if (write_size != read_size) {
                print_error("write: ");
                break;
            }
            debug_info("rank " << rank << " write " << write_size << " in offset_dest " << offset_dest
                               << " from offset_src " << offset_src);
        } while (read_size > 0);

        close(fd_src);
        // unlink(src_path);
        close(fd_dest);
    }

    free(buf);

    debug_info("END entry " << entry << " is_file " << is_file << " dir_name " << dir_name << " dest_prefix "
                            << dest_prefix << " blocksize " << blocksize << " replication_level " << replication_level
                            << " rank " << rank << " size " << size);
    return 0;
}

int flush_list(const char *dir_name, const char *dest_prefix, int blocksize, int replication_level, int rank,
               int size) {
    debug_info("BEGIN dir_name " << dir_name << " dest_prefix " << dest_prefix << " blocksize " << blocksize
                                 << " replication_level " << replication_level << " rank " << rank << " size " << size);

    int ret;
    DIR *dir = NULL;
    struct stat stat_buf;
    char path[PATH_MAX];
    char path_dst[PATH_MAX];
    int buff_coord = 1;

    xpn_partition part("xpn", replication_level, blocksize);
    part.m_data_serv.resize(size);
    std::string aux_str = &src_path[xpn_path_len];
    xpn_file file(aux_str, part);
    file.m_mdata.m_data.fill(file.m_mdata);
    int master_node = file.m_mdata.master_file();
    if (rank == master_node) {
        dir = opendir(dir_name);
        if (dir == NULL) {
            print_error("opendir: "<<dir_name);
            return -1;
        }
        struct dirent *entry;
        entry = readdir(dir);

        while (entry != NULL) {
            if (!strcmp(entry->d_name, ".")) {
                entry = readdir(dir);
                continue;
            }

            if (!strcmp(entry->d_name, "..")) {
                entry = readdir(dir);
                continue;
            }

            sprintf(path, "%s/%s", dir_name, entry->d_name);
            sprintf(path_dst, "%s/%s", dest_prefix, entry->d_name);

            ret = stat(path, &stat_buf);
            if (ret < 0) {
                print_error("stat: "<<path);
                printf("%s\n", path);
                entry = readdir(dir);
                continue;
            }
            lfi_broadcast(&group, master_node, &buff_coord, sizeof(buff_coord));
            lfi_broadcast(&group, master_node, &path, sizeof(path));
            lfi_broadcast(&group, master_node, &path_dst, sizeof(path_dst));
            lfi_broadcast(&group, master_node, &stat_buf, sizeof(stat_buf));
            debug_info("broadcast from " << master_node << " buff_coord " << buff_coord << " path " << path
                                         << " path_dst " << path_dst);
            int is_file = !S_ISDIR(stat_buf.st_mode);
            flush_copy(path, is_file, dir_name, dest_prefix, blocksize, replication_level, rank, size);
            if (!is_file) {
                flush_list(path, path_dst, blocksize, replication_level, rank, size);
            }

            entry = readdir(dir);
        }
        buff_coord = 0;
        lfi_broadcast(&group, master_node, &buff_coord, sizeof(buff_coord));
        closedir(dir);
    } else {
        while (buff_coord == 1) {
            lfi_broadcast(&group, master_node, &buff_coord, sizeof(buff_coord));
            if (buff_coord == 0) break;
            lfi_broadcast(&group, master_node, &path, sizeof(path));
            lfi_broadcast(&group, master_node, &path_dst, sizeof(path_dst));
            lfi_broadcast(&group, master_node, &stat_buf, sizeof(stat_buf));
            debug_info("broadcast from " << master_node << " buff_coord " << buff_coord << " path " << path
                                         << " path_dst " << path_dst);

            int is_file = !S_ISDIR(stat_buf.st_mode);
            flush_copy(path, is_file, dir_name, dest_prefix, blocksize, replication_level, rank, size);
            if (!is_file) {
                flush_list(path, path_dst, blocksize, replication_level, rank, size);
            }
        }
    }

    debug_info("END dir_name " << dir_name << " dest_prefix " << dest_prefix << " blocksize " << blocksize
                               << " replication_level " << replication_level << " rank " << rank << " size " << size);
    return 0;
}

void xpn_server::op_flush(xpn_server_comm &comm, const st_xpn_server_flush_preload &head, int rank_client_id,
                          int tag_client_id) {
    XPN_PROFILE_FUNCTION();
    struct st_xpn_server_status status = {};
    int ret;

    debug_info("[Server=" << serv_name << "] [XPN_SERVER_OPS] [op_flush] >> Begin");

    // TODO: only support fabric
    if (m_params.srv_type != server_type::FABRIC) {
        comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);
        return;
    }

    debug_info("[Server=" << serv_name << "] [XPN_SERVER_OPS] [op_flush] flush(" << head.paths.path1() << ", "
                          << head.paths.path2() << ")");

    xpn_conf conf;
    // TODO: do for more than the first partition
    std::vector<std::string> hostnames;
    std::vector<const char *> hostnames_c_str;
    for (auto &&srv_url : conf.partitions[0].server_urls) {
        std::string server;
        std::tie(std::ignore, server, std::ignore, std::ignore) = xpn_parser::parse(srv_url);
        hostnames.emplace_back(server);
    }

    for (auto &&hostname : hostnames) {
        hostnames_c_str.emplace_back(hostname.c_str());
    }

    ret = lfi_group_create(hostnames_c_str.data(), hostnames_c_str.size(), &group);
    if (ret < 0) {
        status.ret = ret;
        comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);
        return;
    }

    int rank, size;
    lfi_group_rank(&group, &rank);
    lfi_group_size(&group, &size);

    xpn_path_len = strlen(head.paths.path1());
    flush_list(head.paths.path1(), head.paths.path2(), conf.partitions[0].bsize, conf.partitions[0].replication_level,
               rank, size);

    lfi_group_close(&group);

    status.ret = 0;
    comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);

    debug_info("[Server=" << serv_name << "] [XPN_SERVER_OPS] [op_flush] << End");
}

int preload_copy(const char *entry, int is_file, const char *dir_name, const char *dest_prefix, int blocksize,
                 int replication_level, int rank, int size) {
    debug_info("entry " << entry << " is_file " << is_file << " dir_name " << dir_name << " dest_prefix " << dest_prefix
                        << " blocksize " << blocksize << " replication_level " << replication_level << " rank " << rank
                        << " size " << size);
    int ret;

    int fd_src, fd_dest;
    char *buf;
    int buf_len;
    off64_t offset_src;
    off64_t ret_2;
    off_t local_offset;
    off_t local_size = 0;
    int local_server;
    int i;
    ssize_t read_size, write_size;
    struct stat st;

    // Alocate buffer
    buf_len = blocksize;
    buf = (char *)malloc(blocksize);
    if (NULL == buf) {
        print_error("malloc: ");
        return -1;
    }

    // Generate source path
    strcpy(src_path, entry);

    // Generate destination path
    const char *aux_entry = entry + strlen(dir_name);
    sprintf(dest_path, "%s/%s", dest_prefix, aux_entry);

    if (rank == 0) {
        printf("%s -> %s\n", src_path, dest_path);
    }

    ret = stat(src_path, &st);
    if (ret < 0) {
        print_error("stat: "<<src_path);
        free(buf);
        return -1;
    }
    if (!is_file) {
        ret = mkdir(dest_path, st.st_mode);
        if (ret < 0 && errno != EEXIST) {
            print_error("mkdir: "<<dest_path);
            free(buf);
            return -1;
        }
    } else if (is_file) {
        fd_src = open64(src_path, O_RDONLY | O_LARGEFILE);
        if (fd_src < 0) {
            print_error("open 1: "<<src_path);
            free(buf);
            return -1;
        }

        fd_dest = open64(dest_path, O_CREAT | O_WRONLY | O_TRUNC | O_LARGEFILE, st.st_mode);
        if (fd_dest < 0) {
            print_error("open 2: "<<dest_path);
            free(buf);
            return -1;
        }

        // Write header
        xpn_partition part("xpn", replication_level, blocksize);
        part.m_data_serv.resize(size);
        std::string aux_str = &dest_path[xpn_path_len];
        xpn_file file(aux_str, part);
        file.m_mdata.m_data.fill(file.m_mdata);

        char header_buf[xpn_metadata::HEADER_SIZE];
        memset(header_buf, 0, xpn_metadata::HEADER_SIZE);
        write_size = filesystem::write(fd_dest, header_buf, xpn_metadata::HEADER_SIZE);
        if (write_size != xpn_metadata::HEADER_SIZE) {
            print_error("write: ");
            free(buf);
            return -1;
        }

        offset_src = 0;
        do {
            for (i = 0; i <= replication_level; i++) {
                file.map_offset_mdata(offset_src, i, local_offset, local_server);

                if (local_server == rank) {
                    ret_2 = lseek64(fd_src, offset_src, SEEK_SET);
                    if (ret_2 < 0) {
                        print_error("lseek: ");
                        goto finish_copy;
                    }
                    ret_2 = lseek64(fd_dest, local_offset + xpn_metadata::HEADER_SIZE, SEEK_SET);
                    if (ret_2 < 0) {
                        print_error("lseek: ");
                        goto finish_copy;
                    }

                    read_size = filesystem::read(fd_src, buf, buf_len);
                    if (read_size <= 0) {
                        goto finish_copy;
                    }
                    write_size = filesystem::write(fd_dest, buf, read_size);
                    if (write_size != read_size) {
                        print_error("write: ");
                        goto finish_copy;
                    }
                    local_size += write_size;
                }
            }

            offset_src += blocksize;
        } while (write_size > 0);

    finish_copy:
        // Update file size
        file.m_mdata.m_data.file_size = st.st_size;
        // Write mdata only when necesary
        int write_mdata = 0;
        int master_dir = file.m_mdata.master_dir();
        int has_master_dir = 0;
        int aux_serv;
        for (int i = 0; i < replication_level + 1; i++) {
            aux_serv = (file.m_mdata.m_data.first_node + i) % size;
            if (aux_serv == rank) {
                write_mdata = 1;
                break;
            }
        }
        for (int i = 0; i < replication_level + 1; i++) {
            aux_serv = (master_dir + i) % size;
            if (aux_serv == rank) {
                has_master_dir = 1;
                break;
            }
        }

        if (write_mdata == 1) {
            ret_2 = lseek64(fd_dest, 0, SEEK_SET);
            write_size = filesystem::write(fd_dest, &file.m_mdata.m_data, sizeof(file.m_mdata.m_data));
            if (write_size != sizeof(file.m_mdata.m_data)) {
                print_error("write: ");
                free(buf);
                return -1;
            }
            local_size += write_size;
        }

        close(fd_src);
        close(fd_dest);
        if (local_size == 0 && has_master_dir == 0) {
            unlink(dest_path);
        }
    }

    free(buf);
    return 0;
}

int preload_list(const char *dir_name, const char *dest_prefix, int blocksize, int replication_level, int rank,
                 int size) {
    int ret;
    DIR *dir = NULL;
    struct stat stat_buf;
    char path[PATH_MAX];

    dir = opendir(dir_name);
    if (dir == NULL) {
        fprintf(stderr, "opendir error %s %s\n", dir_name, strerror(errno));
        return -1;
    }

    struct dirent *entry;
    entry = readdir(dir);

    while (entry != NULL) {
        if (!strcmp(entry->d_name, ".")) {
            entry = readdir(dir);
            continue;
        }

        if (!strcmp(entry->d_name, "..")) {
            entry = readdir(dir);
            continue;
        }

        sprintf(path, "%s/%s", dir_name, entry->d_name);

        ret = stat(path, &stat_buf);
        if (ret < 0) {
            print_error("stat: "<<path);
            printf("%s\n", path);
            entry = readdir(dir);
            continue;
        }

        int is_file = !S_ISDIR(stat_buf.st_mode);
        preload_copy(path, is_file, dir_name, dest_prefix, blocksize, replication_level, rank, size);

        if (S_ISDIR(stat_buf.st_mode)) {
            char path_dst[PATH_MAX];
            sprintf(path_dst, "%s/%s", dest_prefix, entry->d_name);
            preload_list(path, path_dst, blocksize, replication_level, rank, size);
        }

        entry = readdir(dir);
    }

    closedir(dir);

    return 0;
}

void xpn_server::op_preload(xpn_server_comm &comm, const st_xpn_server_flush_preload &head, int rank_client_id,
                            int tag_client_id) {
    XPN_PROFILE_FUNCTION();
    struct st_xpn_server_status status = {};
    int ret;

    debug_info("[Server=" << serv_name << "] [XPN_SERVER_OPS] [op_preload] >> Begin");

    // TODO: only support fabric
    if (m_params.srv_type != server_type::FABRIC) {
        comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);
        return;
    }

    debug_info("[Server=" << serv_name << "] [XPN_SERVER_OPS] [op_preload] flush(" << head.paths.path1() << ", "
                          << head.paths.path2() << ")");

    xpn_conf conf;
    // TODO: do for more than the first partition
    std::vector<std::string> hostnames;
    std::vector<const char *> hostnames_c_str;
    for (auto &&srv_url : conf.partitions[0].server_urls) {
        std::string server;
        std::tie(std::ignore, server, std::ignore, std::ignore) = xpn_parser::parse(srv_url);
        hostnames.emplace_back(server);
    }

    for (auto &&hostname : hostnames) {
        hostnames_c_str.emplace_back(hostname.c_str());
    }

    ret = lfi_group_create(hostnames_c_str.data(), hostnames_c_str.size(), &group);
    if (ret < 0) {
        status.ret = ret;
        comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);
        return;
    }

    int rank, size;
    lfi_group_rank(&group, &rank);
    lfi_group_size(&group, &size);

    xpn_path_len = strlen(head.paths.path1());
    preload_list(head.paths.path1(), head.paths.path2(), conf.partitions[0].bsize, conf.partitions[0].replication_level,
                 rank, size);

    lfi_group_close(&group);

    status.ret = 0;
    status.server_errno = 0;
    comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);

    debug_info("[Server=" << serv_name << "] [XPN_SERVER_OPS] [op_preload] << End");
}

}  // namespace XPN
