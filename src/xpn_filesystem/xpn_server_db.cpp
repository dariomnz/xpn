
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

#include "xpn_server_db.hpp"

#include <fcntl.h>

#include "base_cpp/debug.hpp"

namespace XPN {

int xpn_server_db::request_block(const std::unique_ptr<xpn_filesystem>& fs, const char* path, int64_t block_offset,
                                 int64_t origin_server_id, xpn_server_block& out_block) {
    debug_info(">> Begin");
    file_map_elem_ref elem(*this, path);
    xpn_server_block block;
    int ret = get_block(fs, path, block_offset, block);
    if (ret == ERROR) {
        debug_info("<< End ERROR");
        return ERROR;
    }

    if (ret == ERROR_NOT_FOUND) {
        xpn_server_last_block last_block;
        get_last_block(fs, path, origin_server_id, last_block);
        out_block.block_offset = block_offset;
        out_block.server_id = origin_server_id;
        out_block.server_block_offset = last_block.last_server_block_offset;
        save_block(fs, path, out_block);
        last_block.last_server_block_offset++;
        save_last_block(fs, path, last_block);
        debug_info("<< End SUCCESS");
        return SUCCESS;
    }

    out_block = block;
    debug_info("<< End SUCCESS");
    return SUCCESS;
}

int xpn_server_db::get_block(const std::unique_ptr<xpn_filesystem>& fs, const char* path, int64_t block_offset,
                             xpn_server_block& out_block) {
    std::string file_db_block = std::string(path) + FILE_EXTENSION_DB_BLOCK;
    debug_info(">> Begin (" << file_db_block << ")");

    int fd =
        fs->open(file_db_block.c_str(), O_RDONLY | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
    if (fd < 0) {
        debug_info("<< End (" << file_db_block << ") ERROR");
        return ERROR;
    }

    int64_t res = fs->pread(fd, &out_block, sizeof(out_block), sizeof(out_block) * block_offset);
    if (res != sizeof(out_block)) {
        if (res == 0) {
            // Not exist
            out_block = xpn_server_block{.block_offset = block_offset, .server_id = -1, .server_block_offset = -1};
            debug_info("<< End (" << file_db_block << ") ERROR_NOT_FOUND");
            return ERROR_NOT_FOUND;
        } else {
            // Error
            fs->close(fd);
            debug_info("<< End (" << file_db_block << ") ERROR");
            return ERROR;
        }
    }

    if (fs->close(fd) < 0) {
        debug_info("<< End (" << file_db_block << ") ERROR");
        return ERROR;
    }
    debug_info("<< End (" << file_db_block << ") SUCCESS");
    return SUCCESS;
}

int xpn_server_db::get_last_block(const std::unique_ptr<xpn_filesystem>& fs, const char* path, int64_t server_id,
                                  xpn_server_last_block& out_block) {
    std::string file_db_block = std::string(path) + FILE_EXTENSION_DB_SERVER;
    debug_info(">> Begin (" << file_db_block << ")");

    int fd =
        fs->open(file_db_block.c_str(), O_RDONLY | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
    if (fd < 0) {
        debug_info("<< End (" << file_db_block << ") ERROR");
        return ERROR;
    }

    int64_t res = fs->pread(fd, &out_block, sizeof(out_block), sizeof(out_block) * server_id);
    if (res != sizeof(out_block)) {
        if (res == 0) {
            // Not exist
            out_block = xpn_server_last_block{.server_id = server_id, .last_server_block_offset = 0};
            debug_info("<< End (" << file_db_block << ") SUCCESS");
            return SUCCESS;
        } else {
            // Error
            fs->close(fd);
            debug_info("<< End (" << file_db_block << ") ERROR");
            return ERROR;
        }
    }

    if (fs->close(fd) < 0) {
        debug_info("<< End (" << file_db_block << ") ERROR");
        return ERROR;
    }
    debug_info("<< End (" << file_db_block << ") SUCCESS");
    return SUCCESS;
}

int xpn_server_db::save_block(const std::unique_ptr<xpn_filesystem>& fs, const char* path,
                              const xpn_server_block& block) {
    std::string file_db_block = std::string(path) + FILE_EXTENSION_DB_BLOCK;
    debug_info(">> Begin (" << file_db_block << ")");

    int fd =
        fs->open(file_db_block.c_str(), O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
    if (fd < 0) {
        debug_info("<< End (" << file_db_block << ") ERROR");
        return ERROR;
    }

    int64_t res = fs->pwrite(fd, &block, sizeof(block), sizeof(block) * block.block_offset);
    if (res != sizeof(block)) {
        fs->close(fd);
        debug_info("<< End (" << file_db_block << ") ERROR");
        return ERROR;
    }

    if (fs->close(fd) < 0) {
        debug_info("<< End (" << file_db_block << ") ERROR");
        return ERROR;
    }
    debug_info("<< End (" << file_db_block << ") SUCCESS");
    return SUCCESS;
}

int xpn_server_db::save_last_block(const std::unique_ptr<xpn_filesystem>& fs, const char* path,
                                   const xpn_server_last_block& block) {
    std::string file_db_block = std::string(path) + FILE_EXTENSION_DB_SERVER;
    debug_info(">> Begin (" << file_db_block << ")");

    constexpr const int mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;

    int fd =
        fs->open(file_db_block.c_str(), O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
    if (fd < 0) {
        debug_info("<< End (" << file_db_block << ") ERROR");
        return ERROR;
    }

    int64_t res = fs->pwrite(fd, &block, sizeof(block), sizeof(block) * block.server_id);
    if (res != sizeof(block)) {
        fs->close(fd);
        debug_info("<< End (" << file_db_block << ") ERROR");
        return ERROR;
    }

    if (fs->close(fd) < 0) {
        debug_info("<< End (" << file_db_block << ") ERROR");
        return ERROR;
    }
    debug_info("<< End (" << file_db_block << ") SUCCESS");
    return SUCCESS;
}

xpn_server_db::file_map_elem_ref::file_map_elem_ref(xpn_server_db& db, const char* path) : db(db), path(path) {
    std::unique_lock lock(db.m_file_map_mutex);
    auto& db_elem = db.m_file_map[path];
    db_elem.mutex.lock();
    db_elem.counter++;
    elem = &db_elem;
}

xpn_server_db::file_map_elem_ref::~file_map_elem_ref() {
    elem->counter--;
    elem->mutex.unlock();
    std::unique_lock lock(db.m_file_map_mutex);
    if (elem->counter == 0) {
        db.m_file_map.erase(path);
    }
}
}  // namespace XPN