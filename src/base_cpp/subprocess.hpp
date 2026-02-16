
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

#include <fcntl.h>
#include <sys/prctl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <string>
#include <vector>

#include "base_cpp/debug.hpp"

namespace XPN {
class subprocess {
   public:
    static bool have_command(const std::string& command) {
        process pro("command", {"-v", command});
        int ret = pro.wait_status();
        if (ret < 0) {
            return false;
        }
        return true;
    }

    class process {
       private:
        pid_t pid = 0;
        bool wait_on_destroy = true;

       public:
        process() {}
        process(const std::string& commandPath, std::vector<std::string> args, bool supress_output = true,
                bool wait_for_start = false) {
            execute(commandPath, args, supress_output, wait_for_start);
        }
        process(const std::string& command, bool supress_output = true, bool wait_for_start = false) {
            execute(command, supress_output, wait_for_start);
        }
        ~process() {
            if (wait_on_destroy) wait_status();
        }

        void set_wait_on_destroy(bool wait) { wait_on_destroy = wait; }

        int wait_status() {
            if (pid == 0) return -1;
            int status;
            ::waitpid(pid, &status, 0);
            return status;
        }

        bool is_running() {
            if (pid == 0) return false;
            int status;
            int ret = ::waitpid(pid, &status, WNOHANG);
            if (ret == 0) {
                return true;
            }
            return false;
        }

        int kill(int signal) {
            if (pid == 0) return -1;
            return ::kill(pid, signal);
        }

        void execute(const std::string& command, bool supress_output = true, bool wait_for_start = false) {
            std::vector<std::string> args;
            uint64_t start = 0, end = 0;
            while ((end = command.find(' ', start)) != std::string::npos) {
                args.push_back(command.substr(start, end - start));
                start = end + 1;
            }
            args.push_back(command.substr(start, command.size() - start));

            std::string commandPath = args[0];
            args.erase(args.begin());
            return execute(commandPath, args, supress_output, wait_for_start);
        }

        void execute(const std::string& commandPath, const std::vector<std::string>& args, bool supress_output = true,
                     bool wait_for_start = false) {
            pid = 0;

            int pipefd[2];
            if (wait_for_start) {
                if (pipe2(pipefd, O_CLOEXEC) == -1) {
                    wait_for_start = false;
                }
            }
            pid = ::fork();
            // child
            if (pid == 0) {
                if (supress_output) {
                    remove_input_output();
                }
                // in case the parent dies send SIGTERM
                ::prctl(PR_SET_PDEATHSIG, SIGTERM);

                std::vector<char*> cargs;
                cargs.reserve(args.size() + 2);
                cargs.emplace_back(const_cast<char*>(commandPath.c_str()));
                for (auto& arg : args) {
                    cargs.emplace_back(const_cast<char*>(arg.c_str()));
                }
                cargs.push_back(nullptr);

                ::execvp(commandPath.c_str(), cargs.data());

                // If reaches this is an error to log
                std::stringstream str;
                for (auto& arg : cargs) {
                    str << "'" << arg << "' ";
                }

                print_error("execvp(" << str.str() << ")");
                ::exit(1);
            } else if (pid > 0) {        // Parent
                if (wait_for_start) {
                    ::close(pipefd[1]);  // Close write end in parent

                    char buffer;
                    while (::read(pipefd[0], &buffer, 1) > 0)
                        ;

                    ::close(pipefd[0]);
                }
            }
        }

       private:
        void remove_input_output() {
            // TODO: think if is necesary to close
            int fd = ::open("/dev/null", O_WRONLY | O_CREAT, 0666);
            ::dup2(fd, STDIN_FILENO);
            ::dup2(fd, STDOUT_FILENO);
            ::dup2(fd, STDERR_FILENO);
        }
    };
};
}  // namespace XPN
