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

#include <atomic>
#include <memory>
#include <ostream>
#include <shared_mutex>
#include <string>
#include <vector>

#include "base_cpp/str_unordered_map.hpp"
#include "xpn_server_filesystem.hpp"

namespace XPN {

enum class NodeType { File, Directory };

struct InMemoryNode {
    NodeType type;
    std::string name;
    struct ::stat stat_data;
    std::shared_mutex node_mutex;  // Protects blocks vector and stat_data

    InMemoryNode(NodeType t, std::string_view n) : type(t), name(n) {
        // Initialize stat_data
        stat_data.st_mode = (t == NodeType::Directory) ? (S_IFDIR | 0777) : (S_IFREG | 0666);
        stat_data.st_nlink = 1;
        stat_data.st_size = 0;
        // Times would need real clock, ignoring for simplicity or setting to 0
    }
    virtual ~InMemoryNode() = default;

    friend std::ostream &operator<<(std::ostream &os, InMemoryNode &node);
};

constexpr size_t HEADER_SIZE = 8192;                // 8 KiB
constexpr size_t MEM_BLOCK_SIZE = 4 * 1024 * 1024;  // 4 MiB

struct InMemoryFile : public InMemoryNode {
    // Vector of pointers to memory blocks
    std::vector<std::unique_ptr<uint8_t[]>> blocks;

    InMemoryFile(std::string_view n) : InMemoryNode(NodeType::File, n) {}
};

struct InMemoryDir : public InMemoryNode {
    // Vector of entries for stable iteration. May contain nullptr for deleted entries.
    std::vector<std::shared_ptr<InMemoryNode>> children_list;
    // Map from name to index in children_list for O(1) lookups.
    str_unordered_map<std::string, size_t> children_map;

    InMemoryDir(std::string_view n) : InMemoryNode(NodeType::Directory, n) {}

    void add_child(std::shared_ptr<InMemoryNode> node);
    void remove_child(std::string_view name);
};

class xpn_server_filesystem_memory : public xpn_server_filesystem {
    struct OpenFile {
        std::shared_ptr<InMemoryFile> file;
        int flags;
        uint64_t pos;
    };

    struct OpenDir {
        std::shared_ptr<InMemoryDir> dir;
        size_t pos;
        struct ::dirent current_dirent;
    };

   public:
    xpn_server_filesystem_memory();
    ~xpn_server_filesystem_memory() override;

    int creat(const char *path, uint32_t mode) override;
    int open(const char *path, int flags) override;
    int open(const char *path, int flags, uint32_t mode) override;
    int close(int fd) override;
    int fsync(int fd) override;
    int unlink(const char *path) override;
    int rename(const char *oldPath, const char *newPath) override;
    int stat(const char *path, struct ::stat *st) override;

    int64_t internal_pwrite(OpenFile &of, const void *data, uint64_t len, uint64_t offset);
    int64_t write(int fd, const void *data, uint64_t len) override;
    int64_t pwrite(int fd, const void *data, uint64_t len, int64_t offset) override;

    int64_t internal_pread(OpenFile &of, void *data, uint64_t len, uint64_t offset);
    int64_t read(int fd, void *data, uint64_t len) override;
    int64_t pread(int fd, void *data, uint64_t len, int64_t offset) override;

    int mkdir(const char *path, uint32_t mode) override;
    ::DIR *opendir(const char *path) override;
    int closedir(::DIR *dir) override;
    int rmdir(const char *path) override;
    struct ::dirent *readdir(::DIR *dir) override;
    int64_t telldir(::DIR *dir) override;
    void seekdir(::DIR *dir, int64_t pos) override;

    int statvfs(const char *path, struct ::statvfs *buff) override;

   private:
    std::shared_ptr<InMemoryDir> root;
    std::shared_mutex open_files_mutex;

    // File descriptor table: maps simulated FD to OpenFile
    std::unordered_map<int, OpenFile> open_files;
    std::atomic<int> next_fd{1000};

    std::shared_ptr<InMemoryNode> resolve_path(std::string_view path, std::shared_ptr<InMemoryDir> &parent,
                                               std::string &name_out);
    std::shared_ptr<InMemoryNode> get_node(std::string_view path);
};

}  // namespace XPN
