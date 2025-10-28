
/*
 *  Copyright 2020-2025 Felix Garcia Carballeira, Diego Camarmas Alonso, Alejandro Calderon Mateos, Dario Muñoz Muñoz
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

#include "xpn.h"

int main(int argc, char *argv[]) {
    int ret = 0;
    if (argc != 2) {
        printf("Usage:\n");
        printf("./%s <destination shared path>\n", argv[0]);
        printf("\n");
        printf("<xpn_server data>  ==>  <destination shared path>\n");
        printf("\n");
        return -1;
    }
    ret = xpn_init();
    if (ret < 0) return ret;
    
    ret = xpn_flush(argv[1]);
    if (ret < 0) return ret;
    
    ret = xpn_destroy();
    if (ret < 0) return ret;
}

/* ................................................................... */
