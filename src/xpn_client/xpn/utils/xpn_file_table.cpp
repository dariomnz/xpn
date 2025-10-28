
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

#include "xpn/xpn_file_table.hpp"

#include "xpn/xpn_api.hpp"

namespace XPN {
xpn_file_table::~xpn_file_table() {
    std::vector<int> keys;
    std::unique_lock lock(m_mutex);
    for (auto [key, file] : m_files) {
        keys.emplace_back(key);
    }
    for (auto &key : keys) {
        XPN_DEBUG("Forgot to close: " << key);
        remove(key);
    }
}

int xpn_file_table::insert(std::shared_ptr<xpn_file> file) {
    int fd;
    std::unique_lock lock(m_mutex);
    if (m_free_keys.empty()) {
        while (has(secuencial_key)) {
            debug_info("Has " << secuencial_key << " increment");
            secuencial_key++;
        }
        fd = secuencial_key;
        debug_info("Get from secuencial_key: " << fd);
    } else {
        fd = *m_free_keys.begin();
        m_free_keys.erase(fd);
        debug_info("Get from free_keys: " << fd);
    }
    debug_info("To insert in: " << fd);
    debug_info("File table before insert: " << file->m_path << "\n" << to_string());
    m_files.emplace(std::make_pair(fd, file));
    debug_info("File table after insert: " << file->m_path << "\n" << to_string());
    return fd;
}

bool xpn_file_table::remove(int fd) {
    std::unique_lock lock(m_mutex);
    int res = m_files.erase(fd);
    if (res == 1) {
        m_free_keys.emplace(fd);
    }
    debug_info("Removed " << fd << " table after remove:\n" << to_string());
    return res == 1 ? true : false;
}

// It must be checked if fd is in the file_table with has(fd)
int xpn_file_table::dup(int fd, int new_fd) {
    int ret = -1;
    std::unique_lock lock(m_mutex);
    auto file = get(fd);
    if (!file) {
        return -1;
    }
    file->m_links++;
    if (new_fd != -1) {
        // Like posix dup2 close silently if its open
        if (fd != new_fd && has(new_fd)) {
            xpn_api::get_instance().close(new_fd);
        }
        m_files.emplace(std::make_pair(new_fd, file));
        m_free_keys.erase(new_fd);
        ret = new_fd;
    } else {
        ret = insert(file);
    }
    debug_info("File table:\n" << to_string());
    return ret;
}

std::string xpn_file_table::to_string() {
    std::unique_lock lock(m_mutex);
    std::stringstream out;
    for (auto &[key, file] : m_files) {
        out << "fd: " << key << " : " << (*file).m_path << std::endl;
    }
    return out.str();
}

void xpn_file_table::clean() {
    std::unique_lock lock(m_mutex);
    std::vector<int> fds_to_close;
    fds_to_close.reserve(m_files.size());
    for (auto &[key, file] : m_files) {
        fds_to_close.emplace_back(key);
    }
    for (auto &fd : fds_to_close) {
        xpn_api::get_instance().close(fd);
    }

    m_files.clear();
    decltype(m_free_keys) empty;
    m_free_keys.swap(empty);
    secuencial_key = 1;
}

void xpn_file_table::init_vfhs(const std::unordered_map<std::string, xpn_partition>& partitions) {
    std::unique_lock lock(m_mutex);
    for (auto &[key, file] : m_files) {
        if (partitions.size() > 1){
            std::cerr << "TODO: not supported more than one partition when reinit vfhs"<<std::endl;
            std::abort();
        }
        auto new_file = xpn_file::change_part(file, partitions.begin()->second);
        file.swap(new_file);
    }
}

void xpn_file_table::clean_vfhs() {
    std::unique_lock lock(m_mutex);
    for (auto &[key, file] : m_files) {
        for (auto &vfh : file->m_data_vfh) {
            vfh.reset();
        }
    }
}
}  // namespace XPN