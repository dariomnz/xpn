
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

#include <algorithm>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "base_cpp/debug.hpp"

namespace XPN {

class args {
   public:
    struct option {
        enum class opt_type { value, flag };
        std::string sort_name;
        std::string long_name;
        std::string help;
        opt_type type = opt_type::flag;
    };

    args(int argc, char* argv[], const std::vector<option>& options, std::string_view extra_help = "")
        : m_args(argv, argv + argc), m_options(options), m_extra_help(extra_help) {
        m_options.emplace_back(help_opt);

        for (auto& opt1 : m_options) {
            for (auto& opt2 : m_options) {
                if (opt1.sort_name == opt2.sort_name && opt1.long_name == opt2.long_name && opt1.help == opt2.help &&
                    opt1.type == opt2.type)
                    continue;

                if (opt1.sort_name == opt2.sort_name) {
                    throw std::invalid_argument("The option " + opt1.sort_name + " is repeated");
                }
                if (opt1.long_name == opt2.long_name) {
                    throw std::invalid_argument("The option " + opt1.long_name + " is repeated");
                }
            }
        }
    }

    int parse() {
        if (has_option(help_opt)) {
            help();
            return -1;
        }
        for (uint64_t i = 0; i < m_args.size(); i++) {
            if (m_args[i][0] == '-') {
                auto it = std::find_if(m_options.begin(), m_options.end(), [arg = m_args[i]](const option& opt) {
                    return arg == opt.sort_name || arg == opt.long_name;
                });
                if (it == m_options.end()) {
                    std::cerr << "Error: parse unknown option " << m_args[i] << std::endl;
                    help();
                    return -1;
                }
                // Check opt with values
                auto& opt = *it;
                if (opt.type == option::opt_type::value) {
                    if (i == (m_args.size() - 1)) {
                        std::cerr << "Error: parse option " << m_args[i] << " need a value" << std::endl;
                        help();
                        return -1;
                    }
                    if ((i + 1) < m_args.size() && m_args[i + 1][0] == '-') {
                        std::cerr << "Error: parse option " << m_args[i] << " need a value, encounter " << m_args[i + 1]
                                  << std::endl;
                        help();
                        return -1;
                    }
                    // Check opt only flag
                } else if (opt.type == option::opt_type::flag) {
                    if ((i + 1) < m_args.size() && m_args[i + 1][0] != '-') {
                        // Check if there are more options
                        if (std::find_if(m_args.begin() + i + 1, m_args.end(),
                                         [](std::string_view arg) { return arg[0] == '-'; }) != m_args.end()) {
                            std::cerr << "Error: parse option " << m_args[i] << " is a flag, encounter value "
                                      << m_args[i + 1] << std::endl;
                            help();
                            return -1;
                        }
                    }
                }
            }
            // debug_info("m_args[" << i << "] " << m_args[i]);
            if (i != 0 && m_args[i][0] != '-') {
                auto it = std::find_if(m_options.begin(), m_options.end(), [arg = m_args[i - 1]](const option& opt) {
                    return arg == opt.sort_name || arg == opt.long_name;
                });
                // The previous is a value or a flag option
                if (m_args[i - 1][0] != '-' || (it != m_options.end() && it->type == option::opt_type::flag)) {
                    // debug_info("m_args_values emplace_back '" << m_args[i] << "'");
                    m_args_values.emplace_back(m_args[i]);
                }
            }
        }
        return 0;
    }

    void help() {
        std::cout << "Usage: " << file_name(std::string(m_args[0]).c_str()) << std::endl;
        std::cout << m_extra_help << std::endl;

        std::vector<std::string> option_lines(m_options.size());
        uint64_t max_line = 0;

        std::string line;
        line.reserve(1024);
        // Construct the lines without help
        for (uint64_t i = 0; i < m_options.size(); i++) {
            auto& line = option_lines[i];
            auto& opt = m_options[i];
            line += "  ";
            line += opt.sort_name;
            if (!opt.sort_name.empty() && !opt.long_name.empty()) {
                line += ", ";
            }
            line += opt.long_name;
            if (opt.type == option::opt_type::value) {
                line += " <value>";
            }
            if (max_line < line.size()) {
                max_line = line.size();
            }
        }
        // Resize to make all lines the same size
        for (uint64_t i = 0; i < m_options.size(); i++) {
            auto& line = option_lines[i];
            auto& opt = m_options[i];

            line.resize(max_line, ' ');

            std::cout << line << "    " << opt.help << std::endl;
        }
    }

    std::string_view get_option(const option& option_name) {
        auto it = std::find_if(m_args.begin(), m_args.end(), [option_name](std::string_view value) {
            return option_name.sort_name == value || option_name.long_name == value;
        });
        auto it_value = it + 1;
        if (it != m_args.end() && it_value != m_args.end()) {
            return *it_value;
        }
        return "";
    }

    bool has_option(const option& option_name) {
        auto it = std::find_if(m_args.begin(), m_args.end(), [option_name](std::string_view value) {
            return option_name.sort_name == value || option_name.long_name == value;
        });
        if (it != m_args.end()) {
            return true;
        }
        return false;
    }

    std::string_view get_arg(uint64_t pos) {
        if (pos >= m_args_values.size()) return "";
        return m_args_values[pos];
    }

   private:
    const std::vector<std::string_view> m_args;
    std::vector<std::string_view> m_args_values;
    std::vector<option> m_options;
    const option help_opt = option{.sort_name = "-h", .long_name = "--help", .help = "Display this help and exit"};

    std::string m_extra_help;
};
}  // namespace XPN