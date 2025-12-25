
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

#include "nfi/nfi_server.hpp"

namespace XPN
{
    // Fordward declaration
    struct xpn_fh;
    class xpn_metadata;

    class nfi_xpn_server : public nfi_server
    {
    public:
        nfi_xpn_server(const xpn_parser &parser) : nfi_server(parser) {}
    public:
        // Operations 
        int nfi_open        (std::string_view path, int flags, mode_t mode, xpn_fh &fho) override;
        int nfi_create      (std::string_view path, mode_t mode, xpn_fh &fho) override;
        int nfi_close       (std::string_view path, const xpn_fh &fh) override;
        int64_t nfi_read    (std::string_view path, const xpn_fh &fh,       char *buffer, int64_t offset, uint64_t size) override;
        int64_t nfi_write   (std::string_view path, const xpn_fh &fh, const char *buffer, int64_t offset, uint64_t size) override;
        int nfi_remove      (std::string_view path, bool is_async) override;
        int nfi_rename      (std::string_view path, std::string_view new_path) override;
        int nfi_getattr     (std::string_view path, struct ::stat &st) override;
        int nfi_setattr     (std::string_view path, struct ::stat &st) override;
        int nfi_mkdir       (std::string_view path, mode_t mode) override;
        int nfi_opendir     (std::string_view path, xpn_fh &fho) override;
        int nfi_readdir     (std::string_view path, xpn_fh &fhd, struct ::dirent &entry) override;
        int nfi_closedir    (std::string_view path, const xpn_fh &fhd) override;
        int nfi_rmdir       (std::string_view path, bool is_async) override;
        int nfi_statvfs     (std::string_view path, struct ::statvfs &inf) override;
        int nfi_read_mdata  (std::string_view path, xpn_metadata &mdata) override;
        int nfi_write_mdata (std::string_view path, const xpn_metadata::data &mdata, bool only_file_size) override;
        int nfi_flush       (const char *path) override;
        int nfi_preload     (const char *path) override;
        int nfi_checkpoint  (const char *path) override;
        int nfi_response    () override;
    };
} // namespace XPN