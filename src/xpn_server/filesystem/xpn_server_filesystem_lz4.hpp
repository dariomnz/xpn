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

#include <lz4.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <mutex>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "xpn_server_filesystem.hpp"

namespace XPN {
struct LockEntry {
    std::mutex mtx;
    size_t ref_count = 0;
};

struct BlockKey {
    dev_t dev;
    ino_t ino;
    int64_t block_id;

    bool operator==(const BlockKey &other) const {
        return dev == other.dev && ino == other.ino && block_id == other.block_id;
    }
};

struct BlockKeyHasher {
    using is_transparent = void;
    size_t operator()(const BlockKey &k) const { return hash_combine(k.dev, k.ino, k.block_id); }
    size_t operator()(std::tuple<dev_t, ino_t, int64_t> t) const {
        return hash_combine(std::get<0>(t), std::get<1>(t), std::get<2>(t));
    }
    size_t operator()(dev_t d, ino_t i, int64_t b) const { return hash_combine(d, i, b); }

   private:
    static size_t hash_combine(dev_t d, ino_t i, int64_t b) {
        // boost::hash_combine
        auto seed = std::hash<dev_t>{}(d);
        seed ^= std::hash<ino_t>{}(i) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        seed ^= std::hash<int64_t>{}(b) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        return seed;
    }
};

struct BlockKeyEqual {
    using is_transparent = void;
    bool operator()(const BlockKey &lhs, const BlockKey &rhs) const { return lhs == rhs; }
    bool operator()(const BlockKey &lhs, std::tuple<dev_t, ino_t, int64_t> rhs) const {
        return lhs.dev == std::get<0>(rhs) && lhs.ino == std::get<1>(rhs) && lhs.block_id == std::get<2>(rhs);
    }
    bool operator()(std::tuple<dev_t, ino_t, int64_t> lhs, std::tuple<dev_t, ino_t, int64_t> rhs) const {
        return std::get<0>(lhs) == std::get<0>(rhs) && std::get<1>(lhs) == std::get<1>(rhs) &&
               std::get<2>(lhs) == std::get<2>(rhs);
    }
    bool operator()(std::tuple<dev_t, ino_t, int64_t> lhs, const BlockKey &rhs) const {
        return rhs.dev == std::get<0>(lhs) && rhs.ino == std::get<1>(lhs) && rhs.block_id == std::get<2>(lhs);
    }
};

class BlockLockManager {
   private:
    std::mutex m_map_mutex;
    std::unordered_map<BlockKey, LockEntry, BlockKeyHasher, BlockKeyEqual> m_locks;

   public:
    std::mutex m_map_files_mutex;
    std::unordered_map<int, xpn_server_filesystem::UniqueFile> m_files;

   public:
    static BlockLockManager &get_instance() {
        static BlockLockManager instance;
        return instance;
    }

    std::mutex *acquire(dev_t dev, ino_t ino, int64_t block_id) {
        std::unique_lock lock(m_map_mutex);

        auto it = m_locks.find(std::make_tuple(dev, ino, block_id));
        if (it == m_locks.end()) {
            it = m_locks.try_emplace(BlockKey{dev, ino, block_id}).first;
        }

        it->second.ref_count++;
        return &(it->second.mtx);
    }

    void release(dev_t dev, ino_t ino, int64_t block_id) {
        std::unique_lock lock(m_map_mutex);

        auto it = m_locks.find(std::make_tuple(dev, ino, block_id));
        if (it != m_locks.end()) {
            it->second.ref_count--;
            if (it->second.ref_count == 0) {
                m_locks.erase(it);
            }
        }
    }

    void close_file(int fd) {
        std::unique_lock lock(m_map_files_mutex);
        m_files.erase(fd);
    }
};

class unique_block_lock {
   private:
    xpn_server_filesystem::UniqueFile m_file;
    int64_t m_block_id;
    std::mutex *m_mtx;

   public:
    unique_block_lock(xpn_server_filesystem *fs, int fd, int64_t block_id) : m_block_id(block_id) {
        auto &instance = BlockLockManager::get_instance();

        {
            std::unique_lock lock(instance.m_map_files_mutex);
            auto it = instance.m_files.find(fd);
            if (it == instance.m_files.end()) {
                m_file = fs->get_unique_file(fd);
            } else {
                m_file = (*it).second;
            }
        }
        m_mtx = instance.acquire(m_file.dev, m_file.ino, m_block_id);
        m_mtx->lock();
    }

    ~unique_block_lock() {
        if (m_mtx) {
            m_mtx->unlock();
            BlockLockManager::get_instance().release(m_file.dev, m_file.ino, m_block_id);
        }
    }

    unique_block_lock(const unique_block_lock &) = delete;
    unique_block_lock &operator=(const unique_block_lock &) = delete;
};

class xpn_server_filesystem_lz4 : public xpn_server_filesystem {
   private:
    xpn_server_filesystem *m_backend;
    const uint32_t LOGICAL_BLOCK_SIZE = 512 * 1024;

    static constexpr uint32_t RAW_HEADER_SIZE = 8192;
    static constexpr uint32_t META_SIZE = sizeof(uint32_t) * 2;

    uint32_t MAX_COMP_SIZE = LZ4_COMPRESSBOUND(LOGICAL_BLOCK_SIZE);
    static constexpr uint32_t ALIGNMENT = 4096;
    uint32_t PHYSICAL_BLOCK_SIZE = (META_SIZE + MAX_COMP_SIZE + (ALIGNMENT - 1)) & ~(ALIGNMENT - 1);

    struct BlockHeader {
        uint32_t compressed_size;
        uint32_t uncompressed_size;
    };

    inline int64_t get_physical_offset(int64_t logical_offset) const {
        if (logical_offset < RAW_HEADER_SIZE) return logical_offset;
        int64_t relative_offset = logical_offset - RAW_HEADER_SIZE;
        return RAW_HEADER_SIZE + ((relative_offset / LOGICAL_BLOCK_SIZE) * PHYSICAL_BLOCK_SIZE);
    }

    inline int compress(const char *src, char *dst, int srcSize, int dstCapacity);
    inline int decompress(const char *src, char *dst, int srcSize, int dstCapacity);

   public:
    explicit xpn_server_filesystem_lz4(xpn_server_filesystem *backend, uint32_t block_size)
        : m_backend(backend), LOGICAL_BLOCK_SIZE(block_size) {}

   public:
    int creat(const char *path, uint32_t mode) override;
    int open(const char *path, int flags) override;
    int open(const char *path, int flags, uint32_t mode) override;
    int close(int fd) override;
    int fsync(int fd) override;
    int unlink(const char *path) override;
    int rename(const char *oldPath, const char *newPath) override;
    int stat(const char *path, struct ::stat *st) override;
    int fstat(int fd, struct ::stat *st) override;

    int64_t pwrite(int fd, const void *data, uint64_t len, int64_t offset) override;
    int64_t pread(int fd, void *data, uint64_t len, int64_t offset) override;

    bool is_aligned_for_direct_io(int64_t offset, uint64_t uncompressed_size) const;
    int64_t pwrite_compressed_block(int fd, const void *comp_buf, uint32_t comp_size, uint32_t uncomp_size,
                                    int64_t offset);
    int64_t pread_compressed_block(int fd, void *comp_buf, int64_t offset, uint32_t &comp_size, uint32_t &uncomp_size);

    int mkdir(const char *path, uint32_t mode) override;
    ::DIR *opendir(const char *path) override;
    int closedir(::DIR *dir) override;
    int rmdir(const char *path) override;
    struct ::dirent *readdir(::DIR *dir) override;
    int64_t telldir(::DIR *dir) override;
    void seekdir(::DIR *dir, int64_t pos) override;

    int statvfs(const char *path, struct ::statvfs *buff) override;
};
}  // namespace XPN