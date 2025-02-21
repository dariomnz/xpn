
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

 #include <iostream>
 #include <cstdlib>
 #include <cstring>
 
 namespace XPN
 {
     class xpn_env
     {
     public:
         template<typename T>
         void parse_env(const char * env, T& value)
         {
             char *endptr;
             int int_value;
     
             char *env_value = std::getenv(env);
             if ((env_value == NULL) || (std::strlen(env_value) == 0)) {
                 return;
             }
 
             int_value = (int) std::strtol(env_value, &endptr, 10);
             if ((endptr == env_value) || (*endptr != '\0'))
             {
                 std::cerr << "Warning: environmental variable '"<<env<<"' with value '"<<env_value<<"' is not a number" << std::endl;
                 return;
             }
 
             value = int_value;
         }
 
         xpn_env()
         {
             parse_env("XPN_SCK_PORT", xpn_sck_port);
             parse_env("XPN_CONTROLLER_SCK_PORT", xpn_controller_sck_port);
             xpn_conf = std::getenv("XPN_CONF");
             parse_env("XPN_DEBUG", xpn_debug);
             parse_env("XPN_PROFILER", xpn_profiler);
             parse_env("XPN_CONNECT_TIMEOUT_MS", xpn_connect_timeout_ms);
             parse_env("XPN_THREAD", xpn_thread);
             parse_env("XPN_LOCALITY", xpn_locality);
             parse_env("XPN_SESSION_DIR", xpn_session_dir);
             parse_env("XPN_SESSION_FILE", xpn_session_file);
             parse_env("XPN_SESSION_CONNECT", xpn_session_connect);
             parse_env("XPN_STATS", xpn_stats);
             parse_env("XPN_SESSION_FILE", xpn_session_file);
             xpn_stats_dir = std::getenv("XPN_STATS_DIR");
         }
         // Delete copy constructor
         xpn_env(const xpn_env&) = delete;
         // Delete copy assignment operator
         xpn_env& operator=(const xpn_env&) = delete;
         // Delete move constructor
         xpn_env(xpn_env&&) = delete;
         // Delete move assignment operator
         xpn_env& operator=(xpn_env&&) = delete;
         int xpn_sck_port = 3456;
         int xpn_controller_sck_port = 34567;
         const char * xpn_conf = nullptr;
         int xpn_debug = 0;
         int xpn_connect_timeout_ms = 5000;
         int xpn_profiler = 0;
         int xpn_thread = 0;
         int xpn_locality = 1;
         int xpn_session_file = 0;
         int xpn_session_dir = 1;
         int xpn_session_connect = 1;
         int xpn_stats = 0;
         const char * xpn_stats_dir = nullptr;
     public:
         static xpn_env& get_instance()
         {
             static xpn_env instance;
             return instance;
         }
     };
 }