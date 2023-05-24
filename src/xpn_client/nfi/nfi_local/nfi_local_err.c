/*
 *  Copyright 2000-2023 Felix Garcia Carballeira, Diego Camarmas Alonso, Alejandro Calderon Mateos, Luis Miguel Sanchez Garcia, Borja Bergua Guerra
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

/**
 * @file nfi_local_err.c
 * @brief File to 'TODO'.
 *
 * File to 'TODO'.
 *
 * @authors Felix Garcia Carballeira, Diego Camarmas Alonso, Alejandro Calderon Mateos, Luis Miguel Sanchez Garcia, Borja Bergua Guerra
 * @date  Jul 22, 2021
 * @bug No known bugs.
 */

/************************************************
 *  ... Includes
 ***********************************************/
#include "nfi_local.h"

extern int errno;

/************************************************
 *  ... Functions
 ***********************************************/
// TODO: this interface must be changed
void local_err(int err)
{
  switch (err)
  {
  case LOCALERR_PARAM:
    errno = -1;
    break;

  case LOCALERR_MEMORY:
    errno = -1;
    break;

  case LOCALERR_URL:
    errno = -1;
    break;

  case LOCALERR_MNTCONNECTION:
    errno = -1;
    break;

  case LOCALERR_MOUNT:
    errno = -1;
    break;

  case LOCALERR_NFSCONNECTION:
    errno = -1;
    break;

  case LOCALERR_GETATTR:
    errno = -1;
    break;
  }
}
