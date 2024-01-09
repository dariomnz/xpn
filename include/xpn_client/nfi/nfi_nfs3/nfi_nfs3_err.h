#ifndef _NFI_NFS3_ERR_H_
#define _NFI_NFS3_ERR_H_

#include "nfs3.h"

enum nfi_nfs3_err{
	NFS3ERR_PARAM = 0,
	NFS3ERR_MEMORY = 1,
	NFS3ERR_URL = 2,
	NFS3ERR_MNTCONNECTION = 3,
	NFS3ERR_MOUNT = 4,
	NFS3ERR_NFSCONNECTION = 5,		
	NFS3ERR_GETATTR = 6,
	NFS3ERR_SETATTR = 7,
	NFS3ERR_LOOKUP = 8,
	NFS3ERR_READ = 9,
	NFS3ERR_WRITE = 10,
	NFS3ERR_CREATE = 11,
	NFS3ERR_REMOVE = 12,
	NFS3ERR_MKDIR = 13,
	NFS3ERR_READDIR = 14,
	NFS3ERR_STATFS = 15,
};


void nfs3_err(int err);


#endif
