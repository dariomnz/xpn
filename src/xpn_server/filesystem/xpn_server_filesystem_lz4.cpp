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
#include "xpn_server_filesystem_lz4.hpp"

#include <sys/uio.h>

#include <optional>

#include "base_cpp/debug.hpp"
#include "base_cpp/filesystem.hpp"

namespace XPN {
inline int xpn_server_filesystem_lz4::compress(const char *src, char *dst, int srcSize, int dstCapacity) {
    return LZ4_compress_fast(src, dst, srcSize, dstCapacity, 10);
}

inline int xpn_server_filesystem_lz4::decompress(const char *src, char *dst, int srcSize, int dstCapacity) {
    return LZ4_decompress_safe(src, dst, srcSize, dstCapacity);
}

int64_t xpn_server_filesystem_lz4::pread(int fd, void *buf, uint64_t len, int64_t offset) {
    debug_info(" >> BEGIN (" << fd << ", " << buf << ", " << len << ", " << offset << ")");
    uint8_t *out_ptr = static_cast<uint8_t *>(buf);
    uint64_t total_read = 0;

    std::unique_ptr<char[]> uncomp_scratch;
    auto comp_scratch = std::make_unique_for_overwrite<char[]>(MAX_COMP_SIZE);

    if (offset < RAW_HEADER_SIZE) {
        uint64_t to_read_raw = std::min(len, (uint64_t)RAW_HEADER_SIZE - offset);
        int64_t res = m_backend->pread(fd, out_ptr, to_read_raw, offset);
        if (res <= 0) {
            debug_info(" << HEADER END (" << fd << ", " << buf << ", " << len << ", " << offset << ") = " << res);
            return res;
        }
        total_read += res;
        if (total_read >= len) {
            debug_info(" << HEADER END (" << fd << ", " << buf << ", " << len << ", " << offset << ") = " << res);
            return res;
        }
    }

    while (total_read < len) {
        int64_t current_logical = offset + total_read;
        int64_t phys_pos = get_physical_offset(current_logical);
        uint32_t block_internal_off = (current_logical - RAW_HEADER_SIZE) % LOGICAL_BLOCK_SIZE;

        BlockHeader header;
        if (m_backend->pread(fd, &header, META_SIZE, phys_pos) != META_SIZE) break;

        if (m_backend->pread(fd, comp_scratch.get(), header.compressed_size, phys_pos + META_SIZE) !=
            (int64_t)header.compressed_size)
            break;

        uint32_t avail_in_block = header.uncompressed_size - block_internal_off;
        uint32_t to_read_now = std::min((uint32_t)(len - total_read), avail_in_block);

        if (block_internal_off == 0 && to_read_now == LOGICAL_BLOCK_SIZE) {
            int decomp_res = decompress(comp_scratch.get(), reinterpret_cast<char *>(out_ptr + total_read),
                                        header.compressed_size, LOGICAL_BLOCK_SIZE);
            if (decomp_res < 0) {
                debug_info(" << END (" << fd << ", " << buf << ", " << len << ", " << offset << ") = " << -1);
                return -1;
            }
            debug_info("Decompress from " << format_bytes(header.compressed_size) << " to "
                                          << format_bytes(LOGICAL_BLOCK_SIZE) << " ratio "
                                          << ((double)header.compressed_size / LOGICAL_BLOCK_SIZE));
        } else {
            if (!uncomp_scratch) uncomp_scratch = std::make_unique_for_overwrite<char[]>(LOGICAL_BLOCK_SIZE);

            int decomp_res =
                decompress(comp_scratch.get(), uncomp_scratch.get(), header.compressed_size, LOGICAL_BLOCK_SIZE);
            debug_info("Decompress from " << format_bytes(header.compressed_size) << " to "
                                          << format_bytes(LOGICAL_BLOCK_SIZE) << " ratio "
                                          << ((double)header.compressed_size / LOGICAL_BLOCK_SIZE));
            if (decomp_res < 0) {
                debug_info(" << END (" << fd << ", " << buf << ", " << len << ", " << offset << ") = " << -1);
                return -1;
            }

            std::memcpy(out_ptr + total_read, uncomp_scratch.get() + block_internal_off, to_read_now);

            debug_info("memcpy " << format_bytes(to_read_now));
        }

        total_read += to_read_now;

        if (header.uncompressed_size < LOGICAL_BLOCK_SIZE) break;
    }
    debug_info(" << END (" << fd << ", " << buf << ", " << len << ", " << offset << ") = " << total_read);
    return total_read;
}

int64_t xpn_server_filesystem_lz4::pwrite(int fd, const void *buf, uint64_t len, int64_t offset) {
    debug_info(" >> BEGIN (" << fd << ", " << buf << ", " << len << ", " << offset << ")");
    const uint8_t *in_ptr = static_cast<const uint8_t *>(buf);
    uint64_t total_written = 0;

    // Function-local buffers to ensure thread isolation
    auto uncomp_scratch = std::make_unique_for_overwrite<char[]>(LOGICAL_BLOCK_SIZE);
    auto comp_scratch = std::make_unique_for_overwrite<char[]>(MAX_COMP_SIZE);

    if (offset < RAW_HEADER_SIZE) {
        uint64_t to_write_raw = std::min(len, (uint64_t)RAW_HEADER_SIZE - offset);
        int64_t res = m_backend->pwrite(fd, in_ptr, to_write_raw, offset);
        if (res <= 0) {
            debug_info(" << END (" << fd << ", " << buf << ", " << len << ", " << offset << ") = " << res);
            return res;
        }
        total_written += res;
        if (total_written >= len) {
            debug_info(" << HEADER END (" << fd << ", " << buf << ", " << len << ", " << offset << ") = " << res);
            return res;
        }
    }

    while (total_written < len) {
        int64_t current_logical = offset + total_written;
        int64_t block_id = (current_logical - RAW_HEADER_SIZE) / LOGICAL_BLOCK_SIZE;
        int64_t phys_pos = get_physical_offset(current_logical);
        uint32_t block_off = (current_logical - RAW_HEADER_SIZE) % LOGICAL_BLOCK_SIZE;
        uint32_t to_write = std::min((uint32_t)(len - total_written), LOGICAL_BLOCK_SIZE - block_off);

        const char *source_ptr = nullptr;
        uint32_t current_uncomp_sz = 0;
        std::optional<unique_block_lock> block_lock;

        if (block_off == 0 && to_write == LOGICAL_BLOCK_SIZE) {
            source_ptr = reinterpret_cast<const char *>(in_ptr + total_written);
            current_uncomp_sz = LOGICAL_BLOCK_SIZE;
        } else {
            block_lock.emplace(this, fd, block_id);
            if (!uncomp_scratch) uncomp_scratch = std::make_unique_for_overwrite<char[]>(LOGICAL_BLOCK_SIZE);

            // Read-Modify-Write (RMW)
            BlockHeader old_h;
            if (m_backend->pread(fd, &old_h, META_SIZE, phys_pos) == META_SIZE) {
                m_backend->pread(fd, comp_scratch.get(), old_h.compressed_size, phys_pos + META_SIZE);
                decompress(comp_scratch.get(), uncomp_scratch.get(), old_h.compressed_size, LOGICAL_BLOCK_SIZE);
                current_uncomp_sz = old_h.uncompressed_size;
                debug_info("Decompress Read-Modify-Write from "
                           << format_bytes(old_h.compressed_size) << " to " << format_bytes(LOGICAL_BLOCK_SIZE)
                           << " ratio " << ((double)LOGICAL_BLOCK_SIZE / old_h.compressed_size));
            }

            std::memcpy(uncomp_scratch.get() + block_off, in_ptr + total_written, to_write);
            debug_info("memcpy " << format_bytes(to_write));
            current_uncomp_sz = std::max(current_uncomp_sz, block_off + to_write);
            source_ptr = uncomp_scratch.get();
        }

        int c_size = compress(source_ptr, comp_scratch.get(), current_uncomp_sz, MAX_COMP_SIZE);
        debug_info("Compress from " << format_bytes(current_uncomp_sz) << " to " << format_bytes(c_size) << " ratio "
                                    << ((double)c_size / current_uncomp_sz));

        BlockHeader new_h = {(uint32_t)c_size, current_uncomp_sz};

        if (m_backend->m_mode == filesystem_mode::disk) {
            struct iovec iov[2] = {{.iov_base = &new_h, .iov_len = META_SIZE},
                                   {.iov_base = comp_scratch.get(), .iov_len = (uint32_t)c_size}};
            auto ret = filesystem::pwritev(fd, iov, 2, phys_pos);
            if (ret < 0) {
                debug_info(" << END (" << fd << ", " << buf << ", " << len << ", " << offset << ") = " << ret);
                return ret;
            }
        } else {
            auto ret = m_backend->pwrite(fd, &new_h, META_SIZE, phys_pos);
            if (ret < 0) {
                debug_info(" << END (" << fd << ", " << buf << ", " << len << ", " << offset << ") = " << ret);
                return ret;
            }
            ret = m_backend->pwrite(fd, comp_scratch.get(), c_size, phys_pos + META_SIZE);
            if (ret < 0) {
                debug_info(" << END (" << fd << ", " << buf << ", " << len << ", " << offset << ") = " << ret);
                return ret;
            }
        }

        total_written += to_write;
    }
    debug_info(" << END (" << fd << ", " << buf << ", " << len << ", " << offset << ") = " << total_written);
    return total_written;
}

bool xpn_server_filesystem_lz4::is_aligned_for_direct_io(int64_t offset, uint64_t uncompressed_size) const {
    const bool greater_than_header = (offset >= RAW_HEADER_SIZE);
    const bool aligned = ((offset - RAW_HEADER_SIZE) % LOGICAL_BLOCK_SIZE == 0);
    const bool size_eq_block = (uncompressed_size == LOGICAL_BLOCK_SIZE);

    const bool result = greater_than_header && aligned && size_eq_block;
    debug_info((result ? "true" : "false")
               << " offset " << offset << " size " << uncompressed_size << " greater than header "
               << (greater_than_header ? "true" : "false") << " aligned " << (aligned ? "true" : "false")
               << " size is block " << (size_eq_block ? "true" : "false"));
    return result;
}

int64_t xpn_server_filesystem_lz4::pread_compressed_block(int fd, void *comp_buf, int64_t offset, uint32_t &comp_size,
                                                          uint32_t &uncomp_size) {
    debug_info(" >> BEGIN (" << fd << ", " << comp_buf << ", " << offset << ")");
    BlockHeader header;
    // 1. Alignment check: Must be exactly aligned to a logical block
    if (offset < RAW_HEADER_SIZE || (offset - RAW_HEADER_SIZE) % LOGICAL_BLOCK_SIZE != 0) {
        debug_info("pread_compressed_block failed: Offset not aligned to logical block.");
        debug_info(" << END (" << fd << ", " << comp_buf << ", " << offset << ") = " << -1);
        return -1;
    }

    int64_t phys_pos = get_physical_offset(offset);

    // 2. Read the header (metadata)
    if (m_backend->pread(fd, &header, META_SIZE, phys_pos) != META_SIZE) {
        debug_info(" << END (" << fd << ", " << comp_buf << ", " << offset << ") = " << -1);
        return -1;
    }
    comp_size = header.compressed_size;
    uncomp_size = header.uncompressed_size;

    // If the logical block is empty, return early
    if (header.uncompressed_size == 0) {
        debug_info(" << END (" << fd << ", " << comp_buf << ", " << offset << ") = " << 0);
        return 0;
    }

    // 3. Read the compressed data directly into the user's buffer
    int64_t bytes_read = m_backend->pread(fd, comp_buf, header.compressed_size, phys_pos + META_SIZE);
    if (bytes_read != (int64_t)header.compressed_size) {
        debug_info(" << END readed (" << bytes_read << ") is not compressed_size (" << header.compressed_size << ") ("
                                      << fd << ", " << comp_buf << ", " << offset << ") = " << -1);
        return -1;
    }

    debug_info("Direct read compressed: " << format_bytes(header.compressed_size)
                                          << " (Uncompressed size: " << format_bytes(header.uncompressed_size) << ")");

    // Return the logical (uncompressed) size to maintain consistency with standard pread
    debug_info(" << END (" << fd << ", " << comp_buf << ", " << offset << ") = " << header.uncompressed_size);
    return header.uncompressed_size;
}

int64_t xpn_server_filesystem_lz4::pwrite_compressed_block(int fd, const void *comp_buf, uint32_t comp_size,
                                                           uint32_t uncomp_size, int64_t offset) {
    debug_info(" >> BEGIN (" << fd << ", " << comp_buf << ", " << comp_size << ", " << uncomp_size << ", " << offset
                             << ")");
    // 1. Alignment check: Must match the start of a logical block
    if (offset < RAW_HEADER_SIZE || (offset - RAW_HEADER_SIZE) % LOGICAL_BLOCK_SIZE != 0) {
        debug_info("pwrite_compressed_block failed: Offset not aligned to logical block.");
        debug_info(" << END (" << fd << ", " << comp_buf << ", " << comp_size << ", " << uncomp_size << ", " << offset
                               << ") = " << -1);
        return -1;
    }

    // 2. Validate maximum sizes
    if (uncomp_size > LOGICAL_BLOCK_SIZE || comp_size > MAX_COMP_SIZE) {
        debug_info("pwrite_compressed_block failed: Sizes exceed allowed limits.");
        debug_info(" << END (" << fd << ", " << comp_buf << ", " << comp_size << ", " << uncomp_size << ", " << offset
                               << ") = " << -1);
        return -1;
    }

    int64_t phys_pos = get_physical_offset(offset);
    BlockHeader new_h = {comp_size, uncomp_size};

    if (m_backend->m_mode == filesystem_mode::disk) {
        struct iovec iov[2] = {{.iov_base = &new_h, .iov_len = META_SIZE},
                               {.iov_base = const_cast<void *>(comp_buf), .iov_len = comp_size}};
        int64_t written = filesystem::pwritev(fd, iov, 2, phys_pos);
        if (written != (int64_t)(META_SIZE + comp_size)) {
            debug_info(" << END (" << fd << ", " << comp_buf << ", " << comp_size << ", " << uncomp_size << ", "
                                   << offset << ") = " << -1);
            return -1;
        }
    } else {
        // 3. Write the new header
        if (m_backend->pwrite(fd, &new_h, META_SIZE, phys_pos) != META_SIZE) {
            debug_info(" << END (" << fd << ", " << comp_buf << ", " << comp_size << ", " << uncomp_size << ", "
                                   << offset << ") = " << -1);
            return -1;
        }

        // 4. Write the already-compressed data directly
        if (m_backend->pwrite(fd, comp_buf, comp_size, phys_pos + META_SIZE) != comp_size) {
            debug_info(" << END (" << fd << ", " << comp_buf << ", " << comp_size << ", " << uncomp_size << ", "
                                   << offset << ") = " << -1);
            return -1;
        }
    }

    debug_info("Direct write compressed: " << format_bytes(comp_size)
                                           << " (Uncompressed size: " << format_bytes(uncomp_size) << ")");

    // Return the logical size written
    debug_info(" << END (" << fd << ", " << comp_buf << ", " << comp_size << ", " << uncomp_size << ", " << offset
                           << ") = " << uncomp_size);
    return uncomp_size;
}

int xpn_server_filesystem_lz4::creat(const char *path, uint32_t mode) { return m_backend->creat(path, mode); }
int xpn_server_filesystem_lz4::open(const char *path, int flags) { return m_backend->open(path, flags); }
int xpn_server_filesystem_lz4::open(const char *path, int flags, uint32_t mode) {
    return m_backend->open(path, flags, mode);
}

int xpn_server_filesystem_lz4::close(int fd) {
    BlockLockManager::get_instance().close_file(fd);
    return m_backend->close(fd);
}

int xpn_server_filesystem_lz4::fsync(int fd) { return m_backend->fsync(fd); }
int xpn_server_filesystem_lz4::unlink(const char *path) { return m_backend->unlink(path); }
int xpn_server_filesystem_lz4::rename(const char *oldPath, const char *newPath) {
    return m_backend->rename(oldPath, newPath);
}
int xpn_server_filesystem_lz4::stat(const char *path, struct ::stat *st) { return m_backend->stat(path, st); }
int xpn_server_filesystem_lz4::fstat(int fd, struct ::stat *st) { return m_backend->fstat(fd, st); }

// --- Directory Operations (Passthrough) ---

int xpn_server_filesystem_lz4::mkdir(const char *path, uint32_t mode) { return m_backend->mkdir(path, mode); }
::DIR *xpn_server_filesystem_lz4::opendir(const char *path) { return m_backend->opendir(path); }
int xpn_server_filesystem_lz4::closedir(::DIR *dir) { return m_backend->closedir(dir); }
int xpn_server_filesystem_lz4::rmdir(const char *path) { return m_backend->rmdir(path); }
struct ::dirent *xpn_server_filesystem_lz4::readdir(::DIR *dir) { return m_backend->readdir(dir); }
int64_t xpn_server_filesystem_lz4::telldir(::DIR *dir) { return m_backend->telldir(dir); }
void xpn_server_filesystem_lz4::seekdir(::DIR *dir, int64_t pos) { m_backend->seekdir(dir, pos); }
int xpn_server_filesystem_lz4::statvfs(const char *path, struct ::statvfs *buff) {
    return m_backend->statvfs(path, buff);
}
}  // namespace XPN