
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

#include "base_cpp/debug.hpp"

#include <fcntl.h>

#include <cmath>

namespace XPN {

std::ostream &operator<<(std::ostream &os, [[maybe_unused]] const get_time_stamp &time_stamp) {
    auto now = std::chrono::high_resolution_clock::now();
    std::time_t actual_time = std::chrono::high_resolution_clock::to_time_t(now);
    std::tm tm = {};
    ::localtime_r(&actual_time, &tm);
    
    auto millisec = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    os << tm.tm_year + 1900 << "-";
    os << std::setw(2) << std::setfill('0') << (tm.tm_mon + 1) << "-";
    os << std::setw(2) << std::setfill('0') << tm.tm_mday << " ";
    os << std::setw(2) << std::setfill('0') << tm.tm_hour << ":";
    os << std::setw(2) << std::setfill('0') << tm.tm_min << ":";
    os << std::setw(2) << std::setfill('0') << tm.tm_sec << ".";
    os << std::setw(3) << std::setfill('0') << millisec.count();
    return os;
}

std::ostream &operator<<(std::ostream &os, const format_open_flags &open_flags) {
    if ((open_flags.m_flags & O_ACCMODE) == O_RDONLY) {
        os << "O_RDONLY ";
    } else if ((open_flags.m_flags & O_ACCMODE) == O_WRONLY) {
        os << "O_WRONLY ";
    } else if ((open_flags.m_flags & O_ACCMODE) == O_RDWR) {
        os << "O_RDWR ";
    }

    os << ((open_flags.m_flags & O_CREAT) ? "O_CREAT " : "");
    os << ((open_flags.m_flags & O_EXCL) ? "O_EXCL " : "");
    os << ((open_flags.m_flags & O_NOCTTY) ? "O_NOCTTY " : "");
    os << ((open_flags.m_flags & O_TRUNC) ? "O_TRUNC " : "");
    os << ((open_flags.m_flags & O_APPEND) ? "O_APPEND " : "");
    os << ((open_flags.m_flags & O_NONBLOCK) ? "O_NONBLOCK " : "");
#ifdef O_DSYNC
    os << ((open_flags.m_flags & O_DSYNC) ? "O_DSYNC " : "");
#endif
#ifdef O_RSYNC
    os << ((open_flags.m_flags & O_RSYNC) ? "O_RSYNC " : "");
#endif
#ifdef O_SYNC
    os << ((open_flags.m_flags & O_SYNC) ? "O_SYNC " : "");
#endif
#ifdef O_ASYNC
    os << ((open_flags.m_flags & O_ASYNC) ? "O_ASYNC " : "");
#endif
#ifdef O_DIRECT
    os << ((open_flags.m_flags & O_DIRECT) ? "O_DIRECT " : "");
#endif
#ifdef O_LARGEFILE
    os << ((open_flags.m_flags & O_LARGEFILE) ? "O_LARGEFILE " : "");
#endif
#ifdef O_DIRECTORY
    os << ((open_flags.m_flags & O_DIRECTORY) ? "O_DIRECTORY " : "");
#endif
#ifdef O_NOFOLLOW
    os << ((open_flags.m_flags & O_NOFOLLOW) ? "O_NOFOLLOW " : "");
#endif
#ifdef O_CLOEXEC
    os << ((open_flags.m_flags & O_CLOEXEC) ? "O_CLOEXEC " : "");
#endif

    return os;
}
std::ostream &operator<<(std::ostream &os, const format_open_mode &open_mode) {
    os << ((open_mode.m_mode & S_IRUSR) ? "r" : "-");
    os << ((open_mode.m_mode & S_IWUSR) ? "w" : "-");
    os << ((open_mode.m_mode & S_IXUSR) ? "x" : "-");
    os << ((open_mode.m_mode & S_IRGRP) ? "r" : "-");
    os << ((open_mode.m_mode & S_IWGRP) ? "w" : "-");
    os << ((open_mode.m_mode & S_IXGRP) ? "x" : "-");
    os << ((open_mode.m_mode & S_IROTH) ? "r" : "-");
    os << ((open_mode.m_mode & S_IWOTH) ? "w" : "-");
    os << ((open_mode.m_mode & S_IXOTH) ? "x" : "-");
    return os;
}

std::ostream &operator<<(std::ostream &os, const format_bytes &fb) {
    static const char *units[] = {"B", "KiB", "MiB", "GiB", "TiB", "PiB"};

    if (fb.bytes == 0) return os << "0 B";

    int i = static_cast<int>(std::floor(std::log(fb.bytes) / std::log(1024)));
    if (i >= 6) i = 5;
    if (i == 0) {
        return os << fb.bytes << " " << units[i];
    } else {
        double value = fb.bytes / std::pow(1024, i);
        auto old_precision = os.precision();

        os << std::fixed << std::setprecision(fb.precision) << value << " " << units[i];
        os.precision(old_precision);
        return os;
    }
}
}  // namespace XPN