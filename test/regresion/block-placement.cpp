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

#include <cstdint>
#include <cstring>
#include <iostream>
#include <string_view>
#include <vector>

#include "base_cpp/debug.hpp"
#include "xpn/xpn_file.hpp"
#include "xpn/xpn_metadata.hpp"
#include "xpn/xpn_partition.hpp"

static int g_checks = 0;
static int g_failures = 0;

#define CHECK_CONTAINS(str, needle)                                                                                 \
    do {                                                                                                            \
        ++g_checks;                                                                                                 \
        std::string_view _vstr = (str);                                                                             \
        std::string_view _vneedle = (needle);                                                                       \
        bool _ok = (_vstr.find(_vneedle) != std::string_view::npos);                                                \
        debug_info("[CHECK-" << (_ok ? "OK" : "FAIL") << "] " << #str << " contains " << #needle << " (\"" << _vstr \
                             << "\" contains \"" << _vneedle << "\")");                                             \
        if (!_ok) {                                                                                                 \
            ++g_failures;                                                                                           \
            std::cerr << "FAIL " << __func__ << ":" << __LINE__ << ": " << #str << " does not contain " << #needle  \
                      << " (\"" << _vstr << "\" vs \"" << _vneedle << "\")\n";                                      \
        }                                                                                                           \
    } while (0)

namespace {

constexpr int64_t BS = 512 * 1024;

// Configure m_data directly (no daemon, no MPI). max_nserv must already
// size part.m_data_serv before the file is created; see tests below.
void setup_mdata(XPN::xpn_file &file, int64_t block_size, int repl, int first_node, const std::vector<int> &nserv,
                 const std::vector<int> &offs) {
    auto &d = file.m_mdata.m_data;
    d = {};
    std::memcpy(d.magic_number, XPN::xpn_metadata::MAGIC_NUMBER, 3);
    d.block_size = static_cast<uint64_t>(block_size);
    d.replication_level = repl;
    d.first_node = first_node;
    d.file_size = static_cast<uint64_t>(64 * block_size);
    for (size_t i = 0; i < nserv.size() && i < XPN::xpn_metadata::MAX_RECONSTURCTIONS; ++i) {
        d.data_nserv[i] = nserv[i];
    }
    for (size_t i = 0; i < offs.size() && i < XPN::xpn_metadata::MAX_RECONSTURCTIONS; ++i) {
        d.offsets[i] = offs[i];
    }
    file.m_part.m_block_size = static_cast<uint64_t>(block_size);
    file.m_part.m_replication_level = repl;
}

void test_expand() {
    XPN::xpn_partition part("xpn", 0, (uint64_t)BS, false);
    part.m_data_serv.resize(1);
    XPN::xpn_file file("/xpn/f", part);
    {
        // Starting point
        setup_mdata(file, BS, 0, 0, {2}, {0});
        auto res = file.m_mdata.to_string_blocks(4);
        auto res_check =
            "Serv  0 | Serv  1\n"
            "------- | -------\n"
            "      0 |       1\n"
            "      2 |       3\n"
            "        |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Expand one server
        setup_mdata(file, BS, 0, 0, {2, 3}, {0, 3});
        auto res = file.m_mdata.to_string_blocks(7);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2\n"
            "------- | ------- | -------\n"
            "      0 |       1 |       6\n"
            "      2 |       3 |        \n"
            "      4 |       5 |        \n"
            "        |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Expand two server
        setup_mdata(file, BS, 0, 0, {2, 3, 5}, {0, 3, 6});
        auto res = file.m_mdata.to_string_blocks(20);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3 | Serv  4\n"
            "------- | ------- | ------- | ------- | -------\n"
            "      0 |       1 |       6 |      10 |      11\n"
            "      2 |       3 |       9 |      15 |      16\n"
            "      4 |       5 |      14 |         |        \n"
            "      7 |       8 |      19 |         |        \n"
            "     12 |      13 |         |         |        \n"
            "     17 |      18 |         |         |        \n"
            "        |         |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
}

void test_expand_repl_1() {
    XPN::xpn_partition part("xpn", 1, (uint64_t)BS, false);
    part.m_data_serv.resize(1);
    XPN::xpn_file file("/xpn/f", part);
    {
        // Starting point
        setup_mdata(file, BS, 1, 0, {2}, {0});
        auto res = file.m_mdata.to_string_blocks(4);
        auto res_check =
            "Serv  0 | Serv  1\n"
            "------- | -------\n"
            "      0 |       0\n"
            "      1 |       1\n"
            "      2 |       2\n"
            "      3 |       3\n"
            "        |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Expand one server
        setup_mdata(file, BS, 1, 0, {2, 3}, {0, 3});
        auto res = file.m_mdata.to_string_blocks(7);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2\n"
            "------- | ------- | -------\n"
            "      0 |       0 |       5\n"
            "      1 |       1 |       6\n"
            "      2 |       2 |        \n"
            "      3 |       3 |        \n"
            "      4 |       4 |        \n"
            "      5 |       6 |        \n"
            "        |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Expand two server
        setup_mdata(file, BS, 1, 0, {2, 3, 5}, {0, 3, 6});
        auto res = file.m_mdata.to_string_blocks(20);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3 | Serv  4\n"
            "------- | ------- | ------- | ------- | -------\n"
            "      0 |       0 |       5 |       8 |       9\n"
            "      1 |       1 |       6 |      11 |      11\n"
            "      2 |       2 |       8 |      13 |      14\n"
            "      3 |       3 |      10 |      16 |      16\n"
            "      4 |       4 |      13 |      18 |      19\n"
            "      5 |       6 |      15 |         |        \n"
            "      7 |       7 |      18 |         |        \n"
            "      9 |      10 |         |         |        \n"
            "     12 |      12 |         |         |        \n"
            "     14 |      15 |         |         |        \n"
            "     17 |      17 |         |         |        \n"
            "     19 |         |         |         |        \n"
            "        |         |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
}

void test_expand_repl_2() {
    XPN::xpn_partition part("xpn", 2, (uint64_t)BS, false);
    part.m_data_serv.resize(1);
    XPN::xpn_file file("/xpn/f", part);
    {
        // Starting point
        setup_mdata(file, BS, 2, 0, {2}, {0});
        auto res = file.m_mdata.to_string_blocks(4);
        auto res_check =
            "Serv  0 | Serv  1\n"
            "------- | -------\n"
            "      0 |       0\n"
            "      0 |       1\n"
            "      1 |       1\n"
            "      2 |       2\n"
            "      2 |       3\n"
            "      3 |       3\n"
            "        |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Expand one server
        setup_mdata(file, BS, 2, 0, {2, 3}, {0, 3});
        auto res = file.m_mdata.to_string_blocks(7);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2\n"
            "------- | ------- | -------\n"
            "      0 |       0 |       4\n"
            "      0 |       1 |       5\n"
            "      1 |       1 |       6\n"
            "      2 |       2 |        \n"
            "      2 |       3 |        \n"
            "      3 |       3 |        \n"
            "      4 |       4 |        \n"
            "      5 |       5 |        \n"
            "      6 |       6 |        \n"
            "        |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Expand two server
        setup_mdata(file, BS, 2, 0, {2, 3, 5}, {0, 3, 6});
        auto res = file.m_mdata.to_string_blocks(20);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3 | Serv  4\n"
            "------- | ------- | ------- | ------- | -------\n"
            "      0 |       0 |       4 |       8 |       8\n"
            "      0 |       1 |       5 |       9 |      10\n"
            "      1 |       1 |       6 |      11 |      11\n"
            "      2 |       2 |       7 |      13 |      13\n"
            "      2 |       3 |       9 |      14 |      15\n"
            "      3 |       3 |      11 |      16 |      16\n"
            "      4 |       4 |      12 |      18 |      18\n"
            "      5 |       5 |      14 |      19 |        \n"
            "      6 |       6 |      16 |         |        \n"
            "      7 |       7 |      17 |         |        \n"
            "      8 |       9 |      19 |         |        \n"
            "     10 |      10 |         |         |        \n"
            "     12 |      12 |         |         |        \n"
            "     13 |      14 |         |         |        \n"
            "     15 |      15 |         |         |        \n"
            "     17 |      17 |         |         |        \n"
            "     18 |      19 |         |         |        \n"
            "        |         |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
}

void test_shrink() {
    XPN::xpn_partition part("xpn", 0, (uint64_t)BS, false);
    part.m_data_serv.resize(1);
    XPN::xpn_file file("/xpn/f", part);
    {
        // Starting point
        setup_mdata(file, BS, 0, 0, {5}, {0});
        auto res = file.m_mdata.to_string_blocks(10);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3 | Serv  4\n"
            "------- | ------- | ------- | ------- | -------\n"
            "      0 |       1 |       2 |       3 |       4\n"
            "      5 |       6 |       7 |       8 |       9\n"
            "        |         |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Shrink serv 2
        setup_mdata(file, BS, 0, 0, {5, -3, 4}, {0, 2, 9});
        auto res = file.m_mdata.to_string_blocks(16);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3\n"
            "------- | ------- | ------- | -------\n"
            "      0 |       1 |       3 |       4\n"
            "      5 |       6 |       8 |       9\n"
            "      2 |       7 |      10 |      11\n"
            "     12 |      13 |      14 |      15\n"
            "        |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Shrink serv 1
        setup_mdata(file, BS, 0, 0, {5, -3, 4, -2, 3}, {0, 2, 9, 4, 15});
        auto res = file.m_mdata.to_string_blocks(21);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2\n"
            "------- | ------- | -------\n"
            "      0 |       3 |       4\n"
            "      5 |       8 |       9\n"
            "      2 |      10 |      11\n"
            "     12 |      14 |      15\n"
            "      1 |       6 |       7\n"
            "     13 |      16 |      17\n"
            "     18 |      19 |      20\n"
            "        |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
}

void test_shrink_repl_1() {
    XPN::xpn_partition part("xpn", 1, (uint64_t)BS, false);
    part.m_data_serv.resize(1);
    XPN::xpn_file file("/xpn/f", part);
    {
        // Starting point
        setup_mdata(file, BS, 1, 0, {5}, {0});
        auto res = file.m_mdata.to_string_blocks(10);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3 | Serv  4\n"
            "------- | ------- | ------- | ------- | -------\n"
            "      0 |       0 |       1 |       1 |       2\n"
            "      2 |       3 |       3 |       4 |       4\n"
            "      5 |       5 |       6 |       6 |       7\n"
            "      7 |       8 |       8 |       9 |       9\n"
            "        |         |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Shrink serv 2
        setup_mdata(file, BS, 1, 0, {5, -3, 4}, {0, 4, 9});
        auto res = file.m_mdata.to_string_blocks(16);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3\n"
            "------- | ------- | ------- | -------\n"
            "      0 |       0 |       1 |       2\n"
            "      2 |       3 |       4 |       4\n"
            "      5 |       5 |       6 |       7\n"
            "      7 |       8 |       9 |       9\n"
            "      1 |       3 |       6 |       8\n"
            "     10 |      10 |      11 |      11\n"
            "     12 |      12 |      13 |      13\n"
            "     14 |      14 |      15 |      15\n"
            "        |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Shrink serv 1
        setup_mdata(file, BS, 1, 0, {5, -3, 4, -2, 3}, {0, 4, 9, 8, 15});
        auto res = file.m_mdata.to_string_blocks(21);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2\n"
            "------- | ------- | -------\n"
            "      0 |       1 |       2\n"
            "      2 |       4 |       4\n"
            "      5 |       6 |       7\n"
            "      7 |       9 |       9\n"
            "      1 |       6 |       8\n"
            "     10 |      11 |      11\n"
            "     12 |      13 |      13\n"
            "     14 |      15 |      15\n"
            "      0 |       3 |       5\n"
            "      8 |       3 |      10\n"
            "     12 |      14 |      16\n"
            "     16 |      17 |      17\n"
            "     18 |      18 |      19\n"
            "     19 |      20 |      20\n"
            "        |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
}

void test_shrink_repl_2() {
    XPN::xpn_partition part("xpn", 2, (uint64_t)BS, false);
    part.m_data_serv.resize(1);
    XPN::xpn_file file("/xpn/f", part);
    {
        // Starting point
        setup_mdata(file, BS, 2, 0, {5}, {0});
        auto res = file.m_mdata.to_string_blocks(10);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3 | Serv  4\n"
            "------- | ------- | ------- | ------- | -------\n"
            "      0 |       0 |       0 |       1 |       1\n"
            "      1 |       2 |       2 |       2 |       3\n"
            "      3 |       3 |       4 |       4 |       4\n"
            "      5 |       5 |       5 |       6 |       6\n"
            "      6 |       7 |       7 |       7 |       8\n"
            "      8 |       8 |       9 |       9 |       9\n"
            "        |         |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Shrink serv 2
        setup_mdata(file, BS, 2, 0, {5, -3, 4}, {0, 6, 9});
        auto res = file.m_mdata.to_string_blocks(16);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3\n"
            "------- | ------- | ------- | -------\n"
            "      0 |       0 |       1 |       1\n"
            "      1 |       2 |       2 |       3\n"
            "      3 |       3 |       4 |       4\n"
            "      5 |       5 |       6 |       6\n"
            "      6 |       7 |       7 |       8\n"
            "      8 |       8 |       9 |       9\n"
            "      0 |       2 |       4 |       5\n"
            "      7 |       9 |      10 |      10\n"
            "     10 |      11 |      11 |      11\n"
            "     12 |      12 |      12 |      13\n"
            "     13 |      13 |      14 |      14\n"
            "     14 |      15 |      15 |      15\n"
            "        |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Shrink serv 1
        setup_mdata(file, BS, 2, 0, {5, -3, 4, -2, 3}, {0, 6, 9, 12, 15});
        auto res = file.m_mdata.to_string_blocks(21);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2\n"
            "------- | ------- | -------\n"
            "      0 |       1 |       1\n"
            "      1 |       2 |       3\n"
            "      3 |       4 |       4\n"
            "      5 |       6 |       6\n"
            "      6 |       7 |       8\n"
            "      8 |       9 |       9\n"
            "      0 |       4 |       5\n"
            "      7 |      10 |      10\n"
            "     10 |      11 |      11\n"
            "     12 |      12 |      13\n"
            "     13 |      14 |      14\n"
            "     14 |      15 |      15\n"
            "      0 |       2 |       3\n"
            "      5 |       7 |       8\n"
            "      2 |       9 |      11\n"
            "     12 |      13 |      15\n"
            "     16 |      16 |      16\n"
            "     17 |      17 |      17\n"
            "     18 |      18 |      18\n"
            "     19 |      19 |      19\n"
            "     20 |      20 |      20\n"
            "        |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
}

void test_shrink_10_to_7() {
    XPN::xpn_partition part("xpn", 0, (uint64_t)BS, false);
    part.m_data_serv.resize(1);
    XPN::xpn_file file("/xpn/f", part);
    {
        // Starting point
        setup_mdata(file, BS, 0, 0, {10}, {0});
        auto res = file.m_mdata.to_string_blocks(20);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3 | Serv  4 | Serv  5 | Serv  6 | Serv  7 | Serv  8 | Serv  9\n"
            "------- | ------- | ------- | ------- | ------- | ------- | ------- | ------- | ------- | -------\n"
            "      0 |       1 |       2 |       3 |       4 |       5 |       6 |       7 |       8 |       9\n"
            "     10 |      11 |      12 |      13 |      14 |      15 |      16 |      17 |      18 |      19\n"
            "        |         |         |         |         |         |         |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Shrink last serv
        setup_mdata(file, BS, 0, 0, {10, -10, 9}, {0, 2, 19});
        auto res = file.m_mdata.to_string_blocks(40);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3 | Serv  4 | Serv  5 | Serv  6 | Serv  7 | Serv  8\n"
            "------- | ------- | ------- | ------- | ------- | ------- | ------- | ------- | -------\n"
            "      0 |       1 |       2 |       3 |       4 |       5 |       6 |       7 |       8\n"
            "     10 |      11 |      12 |      13 |      14 |      15 |      16 |      17 |      18\n"
            "      9 |      19 |      20 |      21 |      22 |      23 |      24 |      25 |      26\n"
            "     27 |      28 |      29 |      30 |      31 |      32 |      33 |      34 |      35\n"
            "     36 |      37 |      38 |      39 |         |         |         |         |        \n"
            "        |         |         |         |         |         |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Shrink last serv
        setup_mdata(file, BS, 0, 0, {10, -10, 9, -9, 8}, {0, 2, 19, 4, 43});
        auto res = file.m_mdata.to_string_blocks(60);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3 | Serv  4 | Serv  5 | Serv  6 | Serv  7\n"
            "------- | ------- | ------- | ------- | ------- | ------- | ------- | -------\n"
            "      0 |       1 |       2 |       3 |       4 |       5 |       6 |       7\n"
            "     10 |      11 |      12 |      13 |      14 |      15 |      16 |      17\n"
            "      9 |      19 |      20 |      21 |      22 |      23 |      24 |      25\n"
            "     27 |      28 |      29 |      30 |      31 |      32 |      33 |      34\n"
            "     36 |      37 |      38 |      39 |      40 |      41 |      42 |      43\n"
            "      8 |      18 |      26 |      35 |      44 |      45 |      46 |      47\n"
            "     48 |      49 |      50 |      51 |      52 |      53 |      54 |      55\n"
            "     56 |      57 |      58 |      59 |         |         |         |        \n"
            "        |         |         |         |         |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Shrink last serv
        setup_mdata(file, BS, 0, 0, {10, -10, 9, -9, 8, -8, 7}, {0, 2, 19, 4, 43, 8, 63});
        auto res = file.m_mdata.to_string_blocks(80);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3 | Serv  4 | Serv  5 | Serv  6\n"
            "------- | ------- | ------- | ------- | ------- | ------- | -------\n"
            "      0 |       1 |       2 |       3 |       4 |       5 |       6\n"
            "     10 |      11 |      12 |      13 |      14 |      15 |      16\n"
            "      9 |      19 |      20 |      21 |      22 |      23 |      24\n"
            "     27 |      28 |      29 |      30 |      31 |      32 |      33\n"
            "     36 |      37 |      38 |      39 |      40 |      41 |      42\n"
            "      8 |      18 |      26 |      35 |      44 |      45 |      46\n"
            "     48 |      49 |      50 |      51 |      52 |      53 |      54\n"
            "     56 |      57 |      58 |      59 |      60 |      61 |      62\n"
            "      7 |      17 |      25 |      34 |      43 |      47 |      55\n"
            "     63 |      64 |      65 |      66 |      67 |      68 |      69\n"
            "     70 |      71 |      72 |      73 |      74 |      75 |      76\n"
            "     77 |      78 |      79 |         |         |         |        \n"
            "        |         |         |         |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
}

void test_shrink_10_to_7_repl_1() {
    XPN::xpn_partition part("xpn", 1, (uint64_t)BS, false);
    part.m_data_serv.resize(1);
    XPN::xpn_file file("/xpn/f", part);
    {
        // Starting point
        setup_mdata(file, BS, 1, 0, {10}, {0});
        auto res = file.m_mdata.to_string_blocks(20);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3 | Serv  4 | Serv  5 | Serv  6 | Serv  7 | Serv  8 | Serv  9\n"
            "------- | ------- | ------- | ------- | ------- | ------- | ------- | ------- | ------- | -------\n"
            "      0 |       0 |       1 |       1 |       2 |       2 |       3 |       3 |       4 |       4\n"
            "      5 |       5 |       6 |       6 |       7 |       7 |       8 |       8 |       9 |       9\n"
            "     10 |      10 |      11 |      11 |      12 |      12 |      13 |      13 |      14 |      14\n"
            "     15 |      15 |      16 |      16 |      17 |      17 |      18 |      18 |      19 |      19\n"
            "        |         |         |         |         |         |         |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Shrink last serv
        setup_mdata(file, BS, 1, 0, {10, -10, 9}, {0, 4, 19});
        auto res = file.m_mdata.to_string_blocks(40);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3 | Serv  4 | Serv  5 | Serv  6 | Serv  7 | Serv  8\n"
            "------- | ------- | ------- | ------- | ------- | ------- | ------- | ------- | -------\n"
            "      0 |       0 |       1 |       1 |       2 |       2 |       3 |       3 |       4\n"
            "      5 |       5 |       6 |       6 |       7 |       7 |       8 |       8 |       9\n"
            "     10 |      10 |      11 |      11 |      12 |      12 |      13 |      13 |      14\n"
            "     15 |      15 |      16 |      16 |      17 |      17 |      18 |      18 |      19\n"
            "      4 |       9 |      14 |      19 |      20 |      20 |      21 |      21 |      22\n"
            "     22 |      23 |      23 |      24 |      24 |      25 |      25 |      26 |      26\n"
            "     27 |      27 |      28 |      28 |      29 |      29 |      30 |      30 |      31\n"
            "     31 |      32 |      32 |      33 |      33 |      34 |      34 |      35 |      35\n"
            "     36 |      36 |      37 |      37 |      38 |      38 |      39 |      39 |        \n"
            "        |         |         |         |         |         |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Shrink last serv
        setup_mdata(file, BS, 1, 0, {10, -10, 9, -9, 8}, {0, 4, 19, 8, 39});
        auto res = file.m_mdata.to_string_blocks(60);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3 | Serv  4 | Serv  5 | Serv  6 | Serv  7\n"
            "------- | ------- | ------- | ------- | ------- | ------- | ------- | -------\n"
            "      0 |       0 |       1 |       1 |       2 |       2 |       3 |       3\n"
            "      5 |       5 |       6 |       6 |       7 |       7 |       8 |       8\n"
            "     10 |      10 |      11 |      11 |      12 |      12 |      13 |      13\n"
            "     15 |      15 |      16 |      16 |      17 |      17 |      18 |      18\n"
            "      4 |       9 |      14 |      19 |      20 |      20 |      21 |      21\n"
            "     22 |      23 |      23 |      24 |      24 |      25 |      25 |      26\n"
            "     27 |      27 |      28 |      28 |      29 |      29 |      30 |      30\n"
            "     31 |      32 |      32 |      33 |      33 |      34 |      34 |      35\n"
            "     36 |      36 |      37 |      37 |      38 |      38 |      39 |      39\n"
            "      4 |       9 |      14 |      19 |      22 |      26 |      31 |      35\n"
            "     40 |      40 |      41 |      41 |      42 |      42 |      43 |      43\n"
            "     44 |      44 |      45 |      45 |      46 |      46 |      47 |      47\n"
            "     48 |      48 |      49 |      49 |      50 |      50 |      51 |      51\n"
            "     52 |      52 |      53 |      53 |      54 |      54 |      55 |      55\n"
            "     56 |      56 |      57 |      57 |      58 |      58 |      59 |      59\n"
            "        |         |         |         |         |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Shrink last serv
        setup_mdata(file, BS, 1, 0, {10, -10, 9, -9, 8, -8, 7}, {0, 4, 19, 8, 39, 15, 59});
        auto res = file.m_mdata.to_string_blocks(80);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3 | Serv  4 | Serv  5 | Serv  6\n"
            "------- | ------- | ------- | ------- | ------- | ------- | -------\n"
            "      0 |       0 |       1 |       1 |       2 |       2 |       3\n"
            "      5 |       5 |       6 |       6 |       7 |       7 |       8\n"
            "     10 |      10 |      11 |      11 |      12 |      12 |      13\n"
            "     15 |      15 |      16 |      16 |      17 |      17 |      18\n"
            "      4 |       9 |      14 |      19 |      20 |      20 |      21\n"
            "     22 |      23 |      23 |      24 |      24 |      25 |      25\n"
            "     27 |      27 |      28 |      28 |      29 |      29 |      30\n"
            "     31 |      32 |      32 |      33 |      33 |      34 |      34\n"
            "     36 |      36 |      37 |      37 |      38 |      38 |      39\n"
            "      4 |       9 |      14 |      19 |      22 |      26 |      31\n"
            "     40 |      40 |      41 |      41 |      42 |      42 |      43\n"
            "     44 |      44 |      45 |      45 |      46 |      46 |      47\n"
            "     48 |      48 |      49 |      49 |      50 |      50 |      51\n"
            "     52 |      52 |      53 |      53 |      54 |      54 |      55\n"
            "     56 |      56 |      57 |      57 |      58 |      58 |      59\n"
            "      3 |       8 |      13 |      18 |      21 |      26 |      30\n"
            "     35 |      39 |      35 |      43 |      47 |      51 |      55\n"
            "     59 |      60 |      60 |      61 |      61 |      62 |      62\n"
            "     63 |      63 |      64 |      64 |      65 |      65 |      66\n"
            "     66 |      67 |      67 |      68 |      68 |      69 |      69\n"
            "     70 |      70 |      71 |      71 |      72 |      72 |      73\n"
            "     73 |      74 |      74 |      75 |      75 |      76 |      76\n"
            "     77 |      77 |      78 |      78 |      79 |      79 |        \n"
            "        |         |         |         |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
}

void test_expand_and_shrink() {
    XPN::xpn_partition part("xpn", 0, (uint64_t)BS, false);
    part.m_data_serv.resize(1);
    XPN::xpn_file file("/xpn/f", part);
    {
        // Starting point
        setup_mdata(file, BS, 0, 0, {3}, {0});
        auto res = file.m_mdata.to_string_blocks(3);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2\n"
            "------- | ------- | -------\n"
            "      0 |       1 |       2\n"
            "        |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Expand one server
        setup_mdata(file, BS, 0, 0, {3, 4}, {0, 2});
        auto res = file.m_mdata.to_string_blocks(7);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3\n"
            "------- | ------- | ------- | -------\n"
            "      0 |       1 |       2 |       6\n"
            "      3 |       4 |       5 |        \n"
            "        |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Shrink serv 2
        setup_mdata(file, BS, 0, 0, {3, 4, -3, 3}, {0, 2, 2, 6});
        auto res = file.m_mdata.to_string_blocks(11);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2\n"
            "------- | ------- | -------\n"
            "      0 |       1 |       6\n"
            "      3 |       4 |       7\n"
            "      2 |       5 |      10\n"
            "      8 |       9 |        \n"
            "        |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Expand one server
        setup_mdata(file, BS, 0, 0, {3, 4, -3, 3, 4}, {0, 2, 2, 6, 10});
        auto res = file.m_mdata.to_string_blocks(15);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3\n"
            "------- | ------- | ------- | -------\n"
            "      0 |       1 |       6 |      14\n"
            "      3 |       4 |       7 |        \n"
            "      2 |       5 |      10 |        \n"
            "      8 |       9 |      13 |        \n"
            "     11 |      12 |         |        \n"
            "        |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
}

void test_expand_and_shrink_repl_1() {
    XPN::xpn_partition part("xpn", 1, (uint64_t)BS, false);
    part.m_data_serv.resize(1);
    XPN::xpn_file file("/xpn/f", part);
    {
        // Starting point
        setup_mdata(file, BS, 1, 0, {3}, {0});
        auto res = file.m_mdata.to_string_blocks(3);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2\n"
            "------- | ------- | -------\n"
            "      0 |       0 |       1\n"
            "      1 |       2 |       2\n"
            "        |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Expand one server
        setup_mdata(file, BS, 1, 0, {3, 4}, {0, 2});
        auto res = file.m_mdata.to_string_blocks(7);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3\n"
            "------- | ------- | ------- | -------\n"
            "      0 |       0 |       1 |       4\n"
            "      1 |       2 |       2 |       6\n"
            "      3 |       3 |       4 |        \n"
            "      5 |       5 |       6 |        \n"
            "        |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Shrink serv 2
        setup_mdata(file, BS, 1, 0, {3, 4, -3, 3}, {0, 2, 4, 6});
        auto res = file.m_mdata.to_string_blocks(11);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2\n"
            "------- | ------- | -------\n"
            "      0 |       0 |       4\n"
            "      1 |       2 |       6\n"
            "      3 |       3 |       4\n"
            "      5 |       5 |       7\n"
            "      1 |       2 |       9\n"
            "      6 |       7 |      10\n"
            "      8 |       8 |        \n"
            "      9 |      10 |        \n"
            "        |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Expand one server
        setup_mdata(file, BS, 1, 0, {3, 4, -3, 3, 4}, {0, 2, 4, 6, 10});
        auto res = file.m_mdata.to_string_blocks(21);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3\n"
            "------- | ------- | ------- | -------\n"
            "      0 |       0 |       4 |      12\n"
            "      1 |       2 |       6 |      14\n"
            "      3 |       3 |       4 |      16\n"
            "      5 |       5 |       7 |      18\n"
            "      1 |       2 |       9 |      20\n"
            "      6 |       7 |      10 |        \n"
            "      8 |       8 |      12 |        \n"
            "      9 |      10 |      14 |        \n"
            "     11 |      11 |      16 |        \n"
            "     13 |      13 |      18 |        \n"
            "     15 |      15 |      20 |        \n"
            "     17 |      17 |         |        \n"
            "     19 |      19 |         |        \n"
            "        |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
}

void test_expand_and_shrink_repl_2() {
    XPN::xpn_partition part("xpn", 2, (uint64_t)BS, false);
    part.m_data_serv.resize(1);
    XPN::xpn_file file("/xpn/f", part);
    {
        // Starting point
        setup_mdata(file, BS, 2, 0, {3}, {0});
        auto res = file.m_mdata.to_string_blocks(3);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2\n"
            "------- | ------- | -------\n"
            "      0 |       0 |       0\n"
            "      1 |       1 |       1\n"
            "      2 |       2 |       2\n"
            "        |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Expand one server
        setup_mdata(file, BS, 2, 0, {3, 4}, {0, 2});
        auto res = file.m_mdata.to_string_blocks(7);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3\n"
            "------- | ------- | ------- | -------\n"
            "      0 |       0 |       0 |       4\n"
            "      1 |       1 |       1 |       5\n"
            "      2 |       2 |       2 |       6\n"
            "      3 |       3 |       3 |        \n"
            "      4 |       4 |       5 |        \n"
            "      5 |       6 |       6 |        \n"
            "        |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Shrink serv 2
        setup_mdata(file, BS, 2, 0, {3, 4, -3, 3}, {0, 2, 6, 6});
        auto res = file.m_mdata.to_string_blocks(11);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2\n"
            "------- | ------- | -------\n"
            "      0 |       0 |       4\n"
            "      1 |       1 |       5\n"
            "      2 |       2 |       6\n"
            "      3 |       3 |       2\n"
            "      4 |       4 |       6\n"
            "      5 |       6 |       7\n"
            "      0 |       1 |       8\n"
            "      3 |       5 |       9\n"
            "      7 |       7 |      10\n"
            "      8 |       8 |        \n"
            "      9 |       9 |        \n"
            "     10 |      10 |        \n"
            "        |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Expand one server
        setup_mdata(file, BS, 2, 0, {3, 4, -3, 3, 4}, {0, 2, 6, 6, 10});
        auto res = file.m_mdata.to_string_blocks(21);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3\n"
            "------- | ------- | ------- | -------\n"
            "      0 |       0 |       4 |      12\n"
            "      1 |       1 |       5 |      13\n"
            "      2 |       2 |       6 |      14\n"
            "      3 |       3 |       2 |      16\n"
            "      4 |       4 |       6 |      17\n"
            "      5 |       6 |       7 |      18\n"
            "      0 |       1 |       8 |      20\n"
            "      3 |       5 |       9 |        \n"
            "      7 |       7 |      10 |        \n"
            "      8 |       8 |      11 |        \n"
            "      9 |       9 |      13 |        \n"
            "     10 |      10 |      14 |        \n"
            "     11 |      11 |      15 |        \n"
            "     12 |      12 |      17 |        \n"
            "     13 |      14 |      18 |        \n"
            "     15 |      15 |      19 |        \n"
            "     16 |      16 |         |        \n"
            "     17 |      18 |         |        \n"
            "     19 |      19 |         |        \n"
            "     20 |      20 |         |        \n"
            "        |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
}

void test_expand_shrink_3_10_4_8_6() {
    XPN::xpn_partition part("xpn", 0, (uint64_t)BS, false);
    part.m_data_serv.resize(1);
    XPN::xpn_file file("/xpn/f", part);
    {
        // Starting point: 3 servers
        setup_mdata(file, BS, 0, 0, {3}, {0});
        auto res = file.m_mdata.to_string_blocks(3);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2\n"
            "------- | ------- | -------\n"
            "      0 |       1 |       2\n"
            "        |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Expand 3 -> 10 servers
        setup_mdata(file, BS, 0, 0, {3, 10}, {0, 2});
        auto res = file.m_mdata.to_string_blocks(13);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3 | Serv  4 | Serv  5 | Serv  6 | Serv  7 | Serv  8 | Serv  9\n"
            "------- | ------- | ------- | ------- | ------- | ------- | ------- | ------- | ------- | -------\n"
            "      0 |       1 |       2 |       6 |       7 |       8 |       9 |      10 |      11 |      12\n"
            "      3 |       4 |       5 |         |         |         |         |         |         |        \n"
            "        |         |         |         |         |         |         |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Shrink 10 -> 9 (remove serv 9, 1 block moved)
        setup_mdata(file, BS, 0, 0, {3, 10, -10, 9}, {0, 2, 1, 12});
        auto res = file.m_mdata.to_string_blocks(21);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3 | Serv  4 | Serv  5 | Serv  6 | Serv  7 | Serv  8\n"
            "------- | ------- | ------- | ------- | ------- | ------- | ------- | ------- | -------\n"
            "      0 |       1 |       2 |       6 |       7 |       8 |       9 |      10 |      11\n"
            "      3 |       4 |       5 |      15 |      16 |      17 |      18 |      19 |      20\n"
            "     12 |      13 |      14 |         |         |         |         |         |        \n"
            "        |         |         |         |         |         |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Shrink 9 -> 8 (remove serv 8, 2 blocks moved)
        setup_mdata(file, BS, 0, 0, {3, 10, -10, 9, -9, 8}, {0, 2, 1, 12, 2, 20});
        auto res = file.m_mdata.to_string_blocks(27);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3 | Serv  4 | Serv  5 | Serv  6 | Serv  7\n"
            "------- | ------- | ------- | ------- | ------- | ------- | ------- | -------\n"
            "      0 |       1 |       2 |       6 |       7 |       8 |       9 |      10\n"
            "      3 |       4 |       5 |      15 |      16 |      17 |      18 |      19\n"
            "     12 |      13 |      14 |      22 |      23 |      24 |      25 |      26\n"
            "     11 |      20 |      21 |         |         |         |         |        \n"
            "        |         |         |         |         |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Shrink 8 -> 7 (remove serv 7, 3 blocks moved)
        setup_mdata(file, BS, 0, 0, {3, 10, -10, 9, -9, 8, -8, 7}, {0, 2, 1, 12, 2, 20, 3, 26});
        auto res = file.m_mdata.to_string_blocks(31);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3 | Serv  4 | Serv  5 | Serv  6\n"
            "------- | ------- | ------- | ------- | ------- | ------- | -------\n"
            "      0 |       1 |       2 |       6 |       7 |       8 |       9\n"
            "      3 |       4 |       5 |      15 |      16 |      17 |      18\n"
            "     12 |      13 |      14 |      22 |      23 |      24 |      25\n"
            "     11 |      20 |      21 |      27 |      28 |      29 |      30\n"
            "     10 |      19 |      26 |         |         |         |        \n"
            "        |         |         |         |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Shrink 7 -> 6 (remove serv 6, 4 blocks moved)
        setup_mdata(file, BS, 0, 0, {3, 10, -10, 9, -9, 8, -8, 7, -7, 6}, {0, 2, 1, 12, 2, 20, 3, 26, 4, 30});
        auto res = file.m_mdata.to_string_blocks(33);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3 | Serv  4 | Serv  5\n"
            "------- | ------- | ------- | ------- | ------- | -------\n"
            "      0 |       1 |       2 |       6 |       7 |       8\n"
            "      3 |       4 |       5 |      15 |      16 |      17\n"
            "     12 |      13 |      14 |      22 |      23 |      24\n"
            "     11 |      20 |      21 |      27 |      28 |      29\n"
            "     10 |      19 |      26 |      30 |      31 |      32\n"
            "      9 |      18 |      25 |         |         |        \n"
            "        |         |         |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Shrink 6 -> 5 (remove serv 5, 5 blocks moved)
        setup_mdata(file, BS, 0, 0, {3, 10, -10, 9, -9, 8, -8, 7, -7, 6, -6, 5},
                    {0, 2, 1, 12, 2, 20, 3, 26, 4, 30, 5, 32});
        auto res = file.m_mdata.to_string_blocks(33);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3 | Serv  4\n"
            "------- | ------- | ------- | ------- | -------\n"
            "      0 |       1 |       2 |       6 |       7\n"
            "      3 |       4 |       5 |      15 |      16\n"
            "     12 |      13 |      14 |      22 |      23\n"
            "     11 |      20 |      21 |      27 |      28\n"
            "     10 |      19 |      26 |      30 |      31\n"
            "      9 |      18 |      25 |      29 |      32\n"
            "      8 |      17 |      24 |         |        \n"
            "        |         |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Shrink 5 -> 4 (remove serv 4, 6 blocks moved)
        setup_mdata(file, BS, 0, 0, {3, 10, -10, 9, -9, 8, -8, 7, -7, 6, -6, 5, -5, 4},
                    {0, 2, 1, 12, 2, 20, 3, 26, 4, 30, 5, 32, 6, 32});
        auto res = file.m_mdata.to_string_blocks(35);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3\n"
            "------- | ------- | ------- | -------\n"
            "      0 |       1 |       2 |       6\n"
            "      3 |       4 |       5 |      15\n"
            "     12 |      13 |      14 |      22\n"
            "     11 |      20 |      21 |      27\n"
            "     10 |      19 |      26 |      30\n"
            "      9 |      18 |      25 |      29\n"
            "      8 |      17 |      24 |      28\n"
            "      7 |      16 |      23 |      34\n"
            "     31 |      32 |      33 |        \n"
            "        |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Expand 4 -> 8 servers
        setup_mdata(file, BS, 0, 0, {3, 10, -10, 9, -9, 8, -8, 7, -7, 6, -6, 5, -5, 4, 8},
                    {0, 2, 1, 12, 2, 20, 3, 26, 4, 30, 5, 32, 6, 32, 34});
        auto res = file.m_mdata.to_string_blocks(43);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3 | Serv  4 | Serv  5 | Serv  6 | Serv  7\n"
            "------- | ------- | ------- | ------- | ------- | ------- | ------- | -------\n"
            "      0 |       1 |       2 |       6 |      39 |      40 |      41 |      42\n"
            "      3 |       4 |       5 |      15 |         |         |         |        \n"
            "     12 |      13 |      14 |      22 |         |         |         |        \n"
            "     11 |      20 |      21 |      27 |         |         |         |        \n"
            "     10 |      19 |      26 |      30 |         |         |         |        \n"
            "      9 |      18 |      25 |      29 |         |         |         |        \n"
            "      8 |      17 |      24 |      28 |         |         |         |        \n"
            "      7 |      16 |      23 |      34 |         |         |         |        \n"
            "     31 |      32 |      33 |      38 |         |         |         |        \n"
            "     35 |      36 |      37 |         |         |         |         |        \n"
            "        |         |         |         |         |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Shrink 8 -> 7 (remove serv 7, 1 block moved)
        setup_mdata(file, BS, 0, 0, {3, 10, -10, 9, -9, 8, -8, 7, -7, 6, -6, 5, -5, 4, 8, -8, 7},
                    {0, 2, 1, 12, 2, 20, 3, 26, 4, 30, 5, 32, 6, 32, 34, 1, 42});
        auto res = file.m_mdata.to_string_blocks(49);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3 | Serv  4 | Serv  5 | Serv  6\n"
            "------- | ------- | ------- | ------- | ------- | ------- | -------\n"
            "      0 |       1 |       2 |       6 |      39 |      40 |      41\n"
            "      3 |       4 |       5 |      15 |      46 |      47 |      48\n"
            "     12 |      13 |      14 |      22 |         |         |        \n"
            "     11 |      20 |      21 |      27 |         |         |        \n"
            "     10 |      19 |      26 |      30 |         |         |        \n"
            "      9 |      18 |      25 |      29 |         |         |        \n"
            "      8 |      17 |      24 |      28 |         |         |        \n"
            "      7 |      16 |      23 |      34 |         |         |        \n"
            "     31 |      32 |      33 |      38 |         |         |        \n"
            "     35 |      36 |      37 |      45 |         |         |        \n"
            "     42 |      43 |      44 |         |         |         |        \n"
            "        |         |         |         |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Shrink 7 -> 6 (remove serv 6, 2 blocks moved)
        setup_mdata(file, BS, 0, 0, {3, 10, -10, 9, -9, 8, -8, 7, -7, 6, -6, 5, -5, 4, 8, -8, 7, -7, 6},
                    {0, 2, 1, 12, 2, 20, 3, 26, 4, 30, 5, 32, 6, 32, 34, 1, 42, 2, 48});
        auto res = file.m_mdata.to_string_blocks(53);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2 | Serv  3 | Serv  4 | Serv  5\n"
            "------- | ------- | ------- | ------- | ------- | -------\n"
            "      0 |       1 |       2 |       6 |      39 |      40\n"
            "      3 |       4 |       5 |      15 |      46 |      47\n"
            "     12 |      13 |      14 |      22 |      51 |      52\n"
            "     11 |      20 |      21 |      27 |         |        \n"
            "     10 |      19 |      26 |      30 |         |        \n"
            "      9 |      18 |      25 |      29 |         |        \n"
            "      8 |      17 |      24 |      28 |         |        \n"
            "      7 |      16 |      23 |      34 |         |        \n"
            "     31 |      32 |      33 |      38 |         |        \n"
            "     35 |      36 |      37 |      45 |         |        \n"
            "     42 |      43 |      44 |      50 |         |        \n"
            "     41 |      48 |      49 |         |         |        \n"
            "        |         |         |         |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
}

void test_first_node_nonzero() {
    XPN::xpn_partition part("xpn", 0, (uint64_t)BS, false);
    part.m_data_serv.resize(1);
    XPN::xpn_file file("/xpn/f", part);
    {
        // Same 3->4->3->4 final generation as test_expand_and_shrink(),
        // but starting the round-robin at server 1.
        setup_mdata(file, BS, 0, 1, {3, 4, -3, 3, 4}, {0, 2, 2, 6, 10});
        auto res = file.m_mdata.to_string_blocks(11);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2\n"
            "------- | ------- | -------\n"
            "      2 |       0 |       5\n"
            "      6 |       3 |       4\n"
            "      7 |       1 |       9\n"
            "     10 |       8 |        \n"
            "        |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
    {
        // Same chain, round-robin starting at server 2.
        setup_mdata(file, BS, 0, 2, {3, 4, -3, 3, 4}, {0, 2, 2, 6, 10});
        auto res = file.m_mdata.to_string_blocks(11);
        auto res_check =
            "Serv  0 | Serv  1 | Serv  2\n"
            "------- | ------- | -------\n"
            "      1 |       2 |       4\n"
            "      5 |       6 |       0\n"
            "      3 |       7 |       8\n"
            "      9 |      10 |        \n"
            "        |         |        \n"
            "Correct: All blocks are present";
        CHECK_CONTAINS(res, res_check);
    }
}

void test_32_servers_large() {
    XPN::xpn_partition part("xpn", 0, (uint64_t)BS, false);
    part.m_data_serv.resize(1);
    XPN::xpn_file file("/xpn/f", part);
    {
        // 32 servers, no malleability, 10000 blocks: 313 rows, fits.
        setup_mdata(file, BS, 0, 0, {8, 16, 32}, {0, 999, 2007});
        auto res = file.m_mdata.to_string_blocks(10000);
        CHECK_CONTAINS(res, "Serv 31");
        CHECK_CONTAINS(res, "Correct: All blocks are present");
        // clang-format off
        CHECK_CONTAINS(res,
            "   3928 |    3929 |    3930 |    3931 |    3932 |    3933 |    3934 |    3935 |    7936 |    7937 |    7938 |    7939 |    7940 |    7941 |    7942 |    7943 |    9960 |    9961 |    9962 |    9963 |    9964 |    9965 |    9966 |    9967 |    9968 |    9969 |    9970 |    9971 |    9972 |    9973 |    9974 |    9975\n"
            "   3960 |    3961 |    3962 |    3963 |    3964 |    3965 |    3966 |    3967 |    7968 |    7969 |    7970 |    7971 |    7972 |    7973 |    7974 |    7975 |    9992 |    9993 |    9994 |    9995 |    9996 |    9997 |    9998 |    9999 |         |         |         |         |         |         |         |        \n"
            "   3992 |    3993 |    3994 |    3995 |    3996 |    3997 |    3998 |    3999 |    8000 |    8001 |    8002 |    8003 |    8004 |    8005 |    8006 |    8007 |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |        \n"
            "   4024 |    4025 |    4026 |    4027 |    4028 |    4029 |    4030 |    4031 |    8032 |    8033 |    8034 |    8035 |    8036 |    8037 |    8038 |    8039 |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |        \n"
        );
        CHECK_CONTAINS(res,
            "   5944 |    5945 |    5946 |    5947 |    5948 |    5949 |    5950 |    5951 |    9952 |    9953 |    9954 |    9955 |    9956 |    9957 |    9958 |    9959 |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |        \n"
            "   5976 |    5977 |    5978 |    5979 |    5980 |    5981 |    5982 |    5983 |    9984 |    9985 |    9986 |    9987 |    9988 |    9989 |    9990 |    9991 |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |        \n"
            "   6008 |    6009 |    6010 |    6011 |    6012 |    6013 |    6014 |    6015 |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |        \n"
            "   6040 |    6041 |    6042 |    6043 |    6044 |    6045 |    6046 |    6047 |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |        \n"
        );
        CHECK_CONTAINS(res,
            "   9912 |    9913 |    9914 |    9915 |    9916 |    9917 |    9918 |    9919 |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |        \n"
            "   9944 |    9945 |    9946 |    9947 |    9948 |    9949 |    9950 |    9951 |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |        \n"
            "   9976 |    9977 |    9978 |    9979 |    9980 |    9981 |    9982 |    9983 |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |        \n"
            "        |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |        \n"
        );
        // clang-format on
    }
    {
        // 32 servers with replication 1, 10000 blocks: 625 rows, fits.
        setup_mdata(file, BS, 1, 0, {8, 16, 32}, {0, 999, 2007});
        auto res = file.m_mdata.to_string_blocks(10000);
        CHECK_CONTAINS(res, "Serv 31");
        CHECK_CONTAINS(res, "Correct: All blocks are present");
        // clang-format off
        CHECK_CONTAINS(res,
            "   3944 |    3944 |    3945 |    3945 |    3946 |    3946 |    3947 |    3947 |    7948 |    7948 |    7949 |    7949 |    7950 |    7950 |    7951 |    7951 |    9968 |    9968 |    9969 |    9969 |    9970 |    9970 |    9971 |    9971 |    9972 |    9972 |    9973 |    9973 |    9974 |    9974 |    9975 |    9975\n"
            "   3960 |    3960 |    3961 |    3961 |    3962 |    3962 |    3963 |    3963 |    7964 |    7964 |    7965 |    7965 |    7966 |    7966 |    7967 |    7967 |    9984 |    9984 |    9985 |    9985 |    9986 |    9986 |    9987 |    9987 |    9988 |    9988 |    9989 |    9989 |    9990 |    9990 |    9991 |    9991\n"
            "   3976 |    3976 |    3977 |    3977 |    3978 |    3978 |    3979 |    3979 |    7980 |    7980 |    7981 |    7981 |    7982 |    7982 |    7983 |    7983 |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |        \n"
            "   3992 |    3992 |    3993 |    3993 |    3994 |    3994 |    3995 |    3995 |    7996 |    7996 |    7997 |    7997 |    7998 |    7998 |    7999 |    7999 |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |        \n"
        );
        CHECK_CONTAINS(res,
            "   5976 |    5976 |    5977 |    5977 |    5978 |    5978 |    5979 |    5979 |    9980 |    9980 |    9981 |    9981 |    9982 |    9982 |    9983 |    9983 |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |        \n"
            "   5992 |    5992 |    5993 |    5993 |    5994 |    5994 |    5995 |    5995 |    9996 |    9996 |    9997 |    9997 |    9998 |    9998 |    9999 |    9999 |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |        \n"
            "   6008 |    6008 |    6009 |    6009 |    6010 |    6010 |    6011 |    6011 |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |        \n"
            "   6024 |    6024 |    6025 |    6025 |    6026 |    6026 |    6027 |    6027 |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |        \n"
        );
        CHECK_CONTAINS(res,
            "   9960 |    9960 |    9961 |    9961 |    9962 |    9962 |    9963 |    9963 |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |        \n"
            "   9976 |    9976 |    9977 |    9977 |    9978 |    9978 |    9979 |    9979 |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |        \n"
            "   9992 |    9992 |    9993 |    9993 |    9994 |    9994 |    9995 |    9995 |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |        \n"
            "        |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |         |        \n"
        );
        // clang-format on
    }
}
}  // namespace

int main() {
    test_expand();
    test_expand_repl_1();
    test_expand_repl_2();
    test_shrink();
    test_shrink_repl_1();
    test_shrink_repl_2();
    test_shrink_10_to_7();
    test_shrink_10_to_7_repl_1();
    test_expand_and_shrink();
    test_expand_and_shrink_repl_1();
    test_expand_and_shrink_repl_2();
    test_expand_shrink_3_10_4_8_6();
    test_first_node_nonzero();
    test_32_servers_large();

    std::cout << "block-placement: " << g_checks << " checks, " << g_failures << " failures\n";
    return g_failures == 0 ? 0 : 1;
}
