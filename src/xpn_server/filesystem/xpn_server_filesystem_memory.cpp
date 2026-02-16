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
#include "xpn_server_filesystem_memory.hpp"

#include <fcntl.h>
#include <sys/sysinfo.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <mutex>
#include <ostream>
#include <shared_mutex>

#include "base_cpp/debug.hpp"

namespace XPN {

void printTree(std::ostream &os, InMemoryNode &node, std::string prefix = "", bool isLast = true) {
    os << prefix;
    os << (isLast ? "└── " : "├── ");

    if (node.type == NodeType::Directory) {
        os << node.name << "/\n";
    } else {
        os << node.name << "\n";
    }

    if (node.type == NodeType::Directory) {
        auto dir = static_cast<InMemoryDir *>(&node);
        std::shared_lock dir_lock(dir->node_mutex);
        if (dir && !dir->children_map.empty()) {
            std::string newPrefix = prefix + (isLast ? "    " : "│   ");

            size_t count = 0;
            for (const auto &pair : dir->children_map) {
                bool lastChild = (++count == dir->children_map.size());
                auto node = dir->children_list[pair.second];
                printTree(os, *node, newPrefix, lastChild);
            }
        }
    }
}

std::ostream &operator<<(std::ostream &os, InMemoryNode &node) {
    if (node.type == NodeType::Directory) {
        os << node.name;
        if (node.name != "/") os << "/";
        os << "\n";
        if (node.type == NodeType::Directory) {
            auto dir = static_cast<InMemoryDir *>(&node);
            size_t count = 0;
            std::shared_lock dir_lock(dir->node_mutex);
            for (const auto &pair : dir->children_map) {
                bool lastChild = (++count == dir->children_map.size());
                auto child = dir->children_list[pair.second];
                printTree(os, *child, "", lastChild);
            }
        }
    } else {
        os << "└── " << node.name << "\n";
    }
    return os;
}

void InMemoryDir::add_child(std::shared_ptr<InMemoryNode> node) {
    size_t idx = children_list.size();
    children_list.push_back(node);
    children_map[node->name] = idx;
}

void InMemoryDir::remove_child(std::string_view name) {
    auto it = children_map.find(name);
    if (it != children_map.end()) {
        size_t index_to_remove = it->second;
        size_t last_index = children_list.size() - 1;

        if (index_to_remove != last_index) {
            auto last_node = children_list.back();
            children_list[index_to_remove] = last_node;
            children_map[last_node->name] = index_to_remove;
        }

        children_list.pop_back();
        children_map.erase(it);
    }
}

xpn_server_filesystem_memory::xpn_server_filesystem_memory() { root = std::make_shared<InMemoryDir>("/"); }

xpn_server_filesystem_memory::~xpn_server_filesystem_memory() {}

std::shared_ptr<InMemoryNode> xpn_server_filesystem_memory::resolve_path(std::string_view path,
                                                                         std::shared_ptr<InMemoryDir> &parent,
                                                                         std::string &name_out) {
    debug_info(" >> BEGIN (" << path << ") \n" << *root);

    if (path.empty()) return nullptr;

    std::string_view p(path);
    if (p.front() == '/') {
        // Absolute path, start from root
        while (!p.empty() && p.front() == '/') {
            p = p.substr(1);
        }
    }

    if (p.empty()) {
        // Path was just "/"
        parent = nullptr;
        name_out = "";
        debug_info(" << END root");
        return root;
    }

    if (p.back() == '/') {
        // Absolute path, start from root
        while (!p.empty() && p.back() == '/') {
            p = p.substr(0, p.size() - 1);
        }
    }

    std::shared_ptr<InMemoryDir> current = root;
    size_t pos = 0;
    std::string token;

    while ((pos = p.find('/')) != std::string::npos) {
        token = p.substr(0, pos);
        p = p.substr(pos + 1);

        if (token.empty()) continue;

        std::shared_lock parent_lock(current->node_mutex);
        auto it = current->children_map.find(token);
        if (it == current->children_map.end()) {
            parent = current;
            name_out = token;
            debug_info(" << END not fount " << token << " nullptr");
            return nullptr;
        }

        auto node = current->children_list[it->second];
        if (node->type != NodeType::Directory) {
            name_out = token;
            debug_info(" << END not directory in path " << token << " nullptr");
            return nullptr;
        }
        current = std::static_pointer_cast<InMemoryDir>(node);
    }

    parent = current;
    name_out = p;

    std::shared_lock parent_lock(current->node_mutex);
    auto it = current->children_map.find(p);
    if (it != current->children_map.end()) {
        debug_info(" << END found " << p);
        return current->children_list[it->second];
    }
    debug_info(" << END not in children " << p << " nullptr");
    return nullptr;
}

std::shared_ptr<InMemoryNode> xpn_server_filesystem_memory::get_node(std::string_view path) {
    std::shared_ptr<InMemoryDir> parent;
    std::string name;
    return resolve_path(path, parent, name);
}

int xpn_server_filesystem_memory::creat(const char *path, uint32_t mode) {
    return open(path, O_CREAT | O_WRONLY | O_TRUNC, mode);
}

int xpn_server_filesystem_memory::open(const char *path, int flags) { return open(path, flags, 0666); }

int xpn_server_filesystem_memory::open(const char *path, int flags, [[maybe_unused]] uint32_t mode) {
    debug_info(" >> BEGIN (" << path << ", " << format_open_flags(flags) << ", " << format_open_mode(mode) << ")");
    std::shared_ptr<InMemoryDir> parent;
    std::string name;
    auto node = resolve_path(path, parent, name);

    if (node) {
        if (flags & O_CREAT && flags & O_EXCL) {
            errno = EEXIST;
            debug_info(" << END -1 EEXIST");
            return -1;
        }
        if (node->type == NodeType::Directory) {
            errno = EISDIR;
            debug_info(" << END -1 EISDIR");
            return -1;
        }
        if (flags & O_TRUNC) {
            auto file = std::static_pointer_cast<InMemoryFile>(node);
            std::unique_lock file_lock(file->node_mutex);
            file->blocks.clear();
            file->stat_data.st_size = 0;
        }
    } else {
        if (flags & O_CREAT) {
            if (!parent) {
                errno = ENOENT;
                debug_info(" << END -1 ENOENT");
                return -1;
            }
            auto new_file = std::make_shared<InMemoryFile>(name);
            {
                std::unique_lock parent_lock(parent->node_mutex);
                parent->add_child(new_file);
            }
            debug_info("Created file: " << name << "\n" << *root);
            node = new_file;
        } else {
            errno = ENOENT;
            debug_info(" << END -1 ENOENT");
            return -1;
        }
    }

    int fd = next_fd++;
    OpenFile of;
    of.file = std::static_pointer_cast<InMemoryFile>(node);
    of.flags = flags;
    of.pos = 0;
    if (flags & O_APPEND) {
        of.pos = of.file->stat_data.st_size;
    }
    std::unique_lock fs_lock(open_files_mutex);
    open_files[fd] = of;
    debug_info(" << END " << fd);
    return fd;
}

int xpn_server_filesystem_memory::close(int fd) {
    debug_info(" >> BEGIN (" << fd << ")");
    std::shared_lock lock(open_files_mutex);
    auto it = open_files.find(fd);
    if (it == open_files.end()) {
        errno = EBADF;
        debug_info(" << END -1 EBADF");
        return -1;
    }
    lock.unlock();
    std::unique_lock fs_lock(open_files_mutex);
    open_files.erase(it);
    debug_info(" << END 0");
    return 0;
}

int xpn_server_filesystem_memory::fsync([[maybe_unused]] int fd) {
    // debug_info(" >> BEGIN");
    // std::shared_lock lock(fs_mutex);
    // if (open_files.find(fd) == open_files.end()) {
    //     errno = EBADF;
    //     debug_info(" << END");
    //     return -1;
    // }
    // debug_info(" << END");
    return 0;
}

int xpn_server_filesystem_memory::unlink(const char *path) {
    debug_info(" >> BEGIN");
    std::shared_ptr<InMemoryDir> parent;
    std::string name;
    auto node = resolve_path(path, parent, name);

    if (!node) {
        errno = ENOENT;
        debug_info(" << END");
        return -1;
    }
    if (node->type == NodeType::Directory) {
        errno = EISDIR;
        debug_info(" << END");
        return -1;
    }
    if (parent) {
        std::unique_lock parent_lock(parent->node_mutex);
        parent->remove_child(name);
    }
    debug_info(" << END");
    return 0;
}

int xpn_server_filesystem_memory::rename(const char *oldPath, const char *newPath) {
    debug_info(" >> BEGIN");
    std::shared_ptr<InMemoryDir> oldParent, newParent;
    std::string oldName, newName;
    auto oldNode = resolve_path(oldPath, oldParent, oldName);

    if (!oldNode) {
        errno = ENOENT;
        debug_info(" << END");
        return -1;
    }

    std::string p = newPath;
    while (!p.empty() && p.front() == '/') p.erase(0, 1);

    std::string p_dir_str;
    std::string p_name_str;
    size_t last_slash = p.find_last_of('/');
    if (last_slash == std::string::npos) {
        p_dir_str = "";
        p_name_str = p;
    } else {
        p_dir_str = p.substr(0, last_slash);
        p_name_str = p.substr(last_slash + 1);
    }

    if (p_dir_str.empty()) {
        newParent = root;
    } else {
        auto node = get_node(p_dir_str);
        if (!node || node->type != NodeType::Directory) {
            errno = ENOENT;
            debug_info(" << END");
            return -1;
        }
        newParent = std::static_pointer_cast<InMemoryDir>(node);
    }
    newName = p_name_str;
    {
        std::unique_lock parent_lock(newParent->node_mutex);
        auto it = newParent->children_map.find(newName);
        if (it != newParent->children_map.end()) {
            auto target = newParent->children_list[it->second];
            if (target->type == NodeType::Directory) {
                if (oldNode->type != NodeType::Directory) {
                    errno = EISDIR;
                    debug_info(" << END");
                    return -1;
                }
                auto dir = std::static_pointer_cast<InMemoryDir>(target);
                if (!dir->children_map.empty()) {
                    errno = ENOTEMPTY;
                    debug_info(" << END");
                    return -1;
                }
            } else {
                if (oldNode->type == NodeType::Directory) {
                    errno = ENOTDIR;
                    debug_info(" << END");
                    return -1;
                }
            }
            newParent->remove_child(newName);
        }
        oldNode->name = newName;
        newParent->add_child(oldNode);
    }

    {
        std::unique_lock parent_lock(oldParent->node_mutex);
        oldParent->remove_child(oldName);
    }

    debug_info(" << END");
    return 0;
}

int xpn_server_filesystem_memory::stat(const char *path, struct ::stat *st) {
    debug_info(" >> BEGIN");
    auto node = get_node(path);
    if (!node) {
        std::string p = path;
        while (!p.empty() && p.front() == '/') p.erase(0, 1);
        if (p.empty()) node = root;
    }

    if (!node) {
        errno = ENOENT;
        debug_info(" << END");
        return -1;
    }

    *st = node->stat_data;
    debug_info(" << END 0");
    return 0;
}

int64_t xpn_server_filesystem_memory::internal_pwrite(OpenFile &of, const void *data, uint64_t len, uint64_t offset) {
    debug_info(" >> BEGIN");
    auto file = of.file;
    uint64_t current_pos = offset;
    uint64_t end_pos = current_pos + len;

    bool needs_growth = false;
    std::shared_lock node_shared_check(file->node_mutex);
    auto get_required_blocks = [](uint64_t pos) -> size_t {
        if (pos == 0) return 0;
        if (pos <= HEADER_SIZE) return 1;
        return 1 + (pos - HEADER_SIZE + MEM_BLOCK_SIZE - 1) / MEM_BLOCK_SIZE;
    };

    size_t required_blocks = get_required_blocks(end_pos);
    if (required_blocks > file->blocks.size() || end_pos > (size_t)file->stat_data.st_size) {
        needs_growth = true;
    }

    if (needs_growth) {
        node_shared_check.unlock();
        {
            std::unique_lock node_unique_lock(file->node_mutex);
            if (required_blocks > file->blocks.size()) {
                size_t old_size = file->blocks.size();
                file->blocks.resize(required_blocks);
                for (size_t i = old_size; i < required_blocks; ++i) {
                    size_t b_size = (i == 0) ? HEADER_SIZE : MEM_BLOCK_SIZE;
                    file->blocks[i] = std::make_unique_for_overwrite<uint8_t[]>(b_size);
                    if (i == 0) {
                        std::memset(file->blocks[i].get(), 0, b_size);
                    }
                }
            }
            if (end_pos > (size_t)file->stat_data.st_size) {
                file->stat_data.st_size = end_pos;
            }
        }
        node_shared_check.lock();
    }

    const uint8_t *src = static_cast<const uint8_t *>(data);
    uint64_t remaining = len;

    while (remaining > 0) {
        size_t block_idx;
        size_t offset_in_block;
        size_t b_size;

        if (current_pos < HEADER_SIZE) {
            block_idx = 0;
            offset_in_block = current_pos;
            b_size = HEADER_SIZE;
        } else {
            block_idx = 1 + (current_pos - HEADER_SIZE) / MEM_BLOCK_SIZE;
            offset_in_block = (current_pos - HEADER_SIZE) % MEM_BLOCK_SIZE;
            b_size = MEM_BLOCK_SIZE;
        }

        size_t to_copy = std::min((size_t)remaining, b_size - offset_in_block);

        std::memcpy(file->blocks[block_idx].get() + offset_in_block, src, to_copy);

        src += to_copy;
        current_pos += to_copy;
        remaining -= to_copy;
    }

    debug_info(" << END " << len);
    return len;
}

int64_t xpn_server_filesystem_memory::write(int fd, const void *data, uint64_t len) {
    debug_info(" >> BEGIN (" << fd << ", " << len << ")");
    std::shared_lock fs_lock(open_files_mutex);
    auto it = open_files.find(fd);
    if (it == open_files.end()) {
        errno = EBADF;
        debug_info(" << END -1 EBADF");
        return -1;
    }

    OpenFile &of = it->second;

    if ((of.flags & O_ACCMODE) == O_RDONLY) {
        errno = EBADF;
        debug_info(" << END -1 EBADF (RDONLY)");
        return -1;
    }

    int64_t res = internal_pwrite(of, data, len, of.pos);

    of.pos += res;

    debug_info(" << END " << res);
    return res;
}

int64_t xpn_server_filesystem_memory::pwrite(int fd, const void *data, uint64_t len, int64_t offset) {
    debug_info(" >> BEGIN (" << fd << ", " << len << ", " << offset << ")");
    std::shared_lock fs_lock(open_files_mutex);
    auto it = open_files.find(fd);
    if (it == open_files.end()) {
        errno = EBADF;
        debug_info(" << END -1 EBADF");
        return -1;
    }

    OpenFile &of = it->second;
    if ((of.flags & O_ACCMODE) == O_RDONLY) {
        errno = EBADF;
        debug_info(" << END -1 EBADF");
        return -1;
    }

    int64_t res = internal_pwrite(of, data, len, offset);

    debug_info(" << END " << res);
    return res;
}

int64_t xpn_server_filesystem_memory::internal_pread(OpenFile &of, void *data, uint64_t len, uint64_t offset) {
    auto file = of.file;
    std::shared_lock node_lock(file->node_mutex);
    if (offset >= (size_t)file->stat_data.st_size) {
        debug_info(" << END 0 (EOF)");
        return 0;
    }

    uint64_t to_read = std::min(len, (uint64_t)file->stat_data.st_size - offset);
    uint64_t current_pos = offset;
    uint64_t remaining = to_read;
    uint8_t *dst = static_cast<uint8_t *>(data);

    while (remaining > 0) {
        size_t block_idx;
        size_t offset_in_block;
        size_t b_size;

        if (current_pos < HEADER_SIZE) {
            block_idx = 0;
            offset_in_block = current_pos;
            b_size = HEADER_SIZE;
        } else {
            block_idx = 1 + (current_pos - HEADER_SIZE) / MEM_BLOCK_SIZE;
            offset_in_block = (current_pos - HEADER_SIZE) % MEM_BLOCK_SIZE;
            b_size = MEM_BLOCK_SIZE;
        }

        size_t to_copy = std::min((size_t)remaining, b_size - offset_in_block);

        if (block_idx < file->blocks.size()) {
            std::memcpy(dst, file->blocks[block_idx].get() + offset_in_block, to_copy);
        } else {
            std::memset(dst, 0, to_copy);  // Should not happen if size is correct
        }

        dst += to_copy;
        current_pos += to_copy;
        remaining -= to_copy;
    }

    debug_info(" << END " << to_read);
    return to_read;
}

int64_t xpn_server_filesystem_memory::read(int fd, void *data, uint64_t len) {
    debug_info(" >> BEGIN (" << fd << ", " << len << ")");
    std::shared_lock fs_lock(open_files_mutex);
    auto it = open_files.find(fd);
    if (it == open_files.end()) {
        errno = EBADF;
        debug_info(" << END -1 EBADF");
        return -1;
    }

    OpenFile &of = it->second;
    if ((of.flags & O_ACCMODE) == O_WRONLY) {
        errno = EBADF;
        debug_info(" << END -1 EBADF");
        return -1;
    }

    int64_t res = internal_pread(of, data, len, of.pos);

    of.pos += res;
    debug_info(" << END " << res);
    return res;
}

int64_t xpn_server_filesystem_memory::pread(int fd, void *data, uint64_t len, int64_t offset) {
    debug_info(" >> BEGIN (" << fd << ", " << len << ", " << offset << ")");
    std::shared_lock fs_lock(open_files_mutex);
    auto it = open_files.find(fd);
    if (it == open_files.end()) {
        errno = EBADF;
        debug_info(" << END -1 EBADF");
        return -1;
    }

    OpenFile &of = it->second;
    if ((of.flags & O_ACCMODE) == O_WRONLY) {
        errno = EBADF;
        debug_info(" << END -1 EBADF");
        return -1;
    }

    int64_t res = internal_pread(of, data, len, offset);

    debug_info(" << END " << res);
    return res;
}

int xpn_server_filesystem_memory::mkdir(const char *path, [[maybe_unused]] uint32_t mode) {
    debug_info(" >> BEGIN (" << path << ")");
    std::shared_ptr<InMemoryDir> parent;
    std::string name;
    auto node = resolve_path(path, parent, name);

    if (node) {
        errno = EEXIST;
        debug_info(" << END -1 EEXIST");
        return -1;
    }
    if (!parent) {
        errno = ENOENT;
        debug_info(" << END -1 ENOENT");
        return -1;
    }

    auto new_dir = std::make_shared<InMemoryDir>(name);
    {
        std::unique_lock parent_lock(parent->node_mutex);
        parent->add_child(new_dir);
    }
    debug_info("Created dir: '" << name << "'\n" << *root);
    debug_info(" << END 0");
    return 0;
}

::DIR *xpn_server_filesystem_memory::opendir(const char *path) {
    debug_info(" >> BEGIN (" << path << ")");
    auto node = get_node(path);
    if (!node && (std::string(path) == "/" || std::string(path).empty())) {
        node = root;
    }

    if (!node || node->type != NodeType::Directory) {
        errno = ENOTDIR;
        debug_info(" << END nullptr ENOTDIR");
        return nullptr;
    }

    OpenDir *od = new OpenDir();
    od->dir = std::static_pointer_cast<InMemoryDir>(node);
    od->pos = 0;

    debug_info(" << END " << reinterpret_cast<void *>(od));
    return reinterpret_cast<::DIR *>(od);
}

int xpn_server_filesystem_memory::closedir(::DIR *dir) {
    debug_info(" >> BEGIN (" << dir << ")");
    // No fs_mutex lock needed here, as it only operates on the OpenDir object itself.
    if (!dir) {
        debug_info(" << END -1");
        return -1;
    }
    OpenDir *od = reinterpret_cast<OpenDir *>(dir);
    delete od;
    debug_info(" << END 0");
    return 0;
}

int xpn_server_filesystem_memory::rmdir(const char *path) {
    debug_info(" >> BEGIN (" << path << ")");
    std::shared_ptr<InMemoryDir> parent;
    std::string name;
    auto node = resolve_path(path, parent, name);

    if (!node) {
        errno = ENOENT;
        debug_info(" << END -1 ENOENT");
        return -1;
    }
    if (node->type != NodeType::Directory) {
        errno = ENOTDIR;
        debug_info(" << END -1 ENOTDIR");
        return -1;
    }
    auto d = std::static_pointer_cast<InMemoryDir>(node);
    {
        std::shared_lock d_lock(d->node_mutex);
        if (!d->children_map.empty()) {
            errno = ENOTEMPTY;
            debug_info(" << END -1 ENOTEMPTY");
            return -1;
        }
    }
    if (!parent) {
        errno = EBUSY;  // Cannot remove root
        debug_info(" << END -1 EBUSY");
        return -1;
    }
    std::unique_lock parent_lock(parent->node_mutex);
    parent->remove_child(name);
    debug_info(" << END 0");
    return 0;
}

struct ::dirent *xpn_server_filesystem_memory::readdir(::DIR *dir) {
    debug_info(" >> BEGIN (" << dir << ")");
    if (!dir) {
        debug_info(" << END nullptr");
        return nullptr;
    }
    OpenDir *od = reinterpret_cast<OpenDir *>(dir);

    std::shared_lock dir_lock(od->dir->node_mutex);
    if (od->pos < od->dir->children_list.size()) {
        auto child = od->dir->children_list[od->pos++];

        std::memset(&od->current_dirent, 0, sizeof(struct ::dirent));
        od->current_dirent.d_ino = 1;
        std::strncpy(od->current_dirent.d_name, child->name.c_str(), sizeof(od->current_dirent.d_name) - 1);

        if (child->type == NodeType::Directory) {
            od->current_dirent.d_type = DT_DIR;
        } else {
            od->current_dirent.d_type = DT_REG;
        }

        debug_info(" << END " << od->current_dirent.d_name);
        return &od->current_dirent;
    }

    debug_info(" << END nullptr (EOF)");
    return nullptr;
}

int64_t xpn_server_filesystem_memory::telldir(::DIR *dir) {
    debug_info(" >> BEGIN (" << dir << ")");
    if (!dir) return -1;
    OpenDir *od = reinterpret_cast<OpenDir *>(dir);
    debug_info(" << END " << od->pos);
    return od->pos;
}

void xpn_server_filesystem_memory::seekdir(::DIR *dir, int64_t pos) {
    debug_info(" >> BEGIN (" << dir << ", " << pos << ")");
    if (!dir) return;
    OpenDir *od = reinterpret_cast<OpenDir *>(dir);
    if (pos < 0)
        od->pos = 0;
    else
        od->pos = static_cast<size_t>(pos);
    debug_info(" << END");
}

int xpn_server_filesystem_memory::statvfs([[maybe_unused]] const char *path, struct ::statvfs *buff) {
    debug_info(" >> BEGIN (" << path << ")");
    if (!buff) {
        debug_info(" << END -1");
        return -1;
    }

    struct sysinfo si;
    if (sysinfo(&si) != 0) {
        debug_info(" << END -1");
        return -1;
    }

    std::memset(buff, 0, sizeof(struct ::statvfs));

    unsigned long mem_unit = si.mem_unit;
    if (mem_unit == 0) mem_unit = 1;

    buff->f_bsize = 4096;
    buff->f_frsize = 4096;

    // Report RAM as "disk" blocks
    buff->f_blocks = (uint64_t)si.totalram * mem_unit / buff->f_bsize;
    buff->f_bfree = (uint64_t)si.freeram * mem_unit / buff->f_bsize;
    buff->f_bavail = buff->f_bfree;

    // Estimate inodes
    buff->f_files = buff->f_blocks / 4;
    buff->f_ffree = buff->f_bfree / 4;
    buff->f_favail = buff->f_ffree;

    buff->f_namemax = 255;

    debug_info(" << END 0");
    return 0;
}

}  // namespace XPN
