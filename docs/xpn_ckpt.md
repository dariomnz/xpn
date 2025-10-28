# xpn_ckpt
## xpn_launch
### 1 Eviroment
```bash

module load mpich
cd $HOME/dariomnz/src/mana/
export MANA_ROOT=$PWD
export PATH=$MANA_ROOT/bin:$PATH
export LD_LIBRARY_PATH=$HOME/dariomnz/bin/xpn/lib/:$LD_LIBRARY_PATH
export PATH=$HOME/dariomnz/bin/xpn/bin/:$PATH
export LFI_PATH=$HOME/dariomnz/bin/xpn/

export XPN_CKPT_DIR=/dev/shm/xpn_ckpt
export XPN_CONF=$HOME/dariomnz/src/mana/xpn_conf.conf

```

### 2 Launch coordinator
```bash
mana_coordinator
```

### 3 Launch server
```bash
# Create dirs
mpiexec -n 2 -hosts srv101,srv102 -l mkdir -p /dev/shm/xpn_ckpt /dev/shm/xpn/ckpt

# Clean dirs and launch with logs files
mpiexec -n 2 -hosts srv101,srv102 -l rm -r /dev/shm/xpn_ckpt /dev/shm/xpn && mpiexec -n 2 -hosts srv101,srv102 -l mkdir -p /dev/shm/xpn_ckpt /dev/shm/xpn/ckpt && mpiexec -n 2 -hosts srv101,srv102 -l -outfile-pattern ~/dariomnz/xpn_server.out-%r.log -errfile-pattern ~/dariomnz/xpn_server.err-%r.log xpn_server --server_type fabric --thread_mode pool

# Clean dirs and launch fabric
mpiexec -n 2 -hosts srv101,srv102 -l rm -r /dev/shm/xpn_ckpt /dev/shm/xpn && mpiexec -n 2 -hosts srv101,srv102 -l mkdir -p /dev/shm/xpn_ckpt /dev/shm/xpn/ckpt && mpiexec -n 2 -hosts srv101,srv102 -l xpn_server --server_type fabric --thread_mode pool

# Clean dirs and launch sck
mpiexec -n 2 -hosts srv101,srv102 -l rm -r /dev/shm/xpn_ckpt /dev/shm/xpn && mpiexec -n 2 -hosts srv101,srv102 -l mkdir -p /dev/shm/xpn_ckpt /dev/shm/xpn/ckpt && mpiexec -n 2 -hosts srv101,srv102 -l xpn_server --server_type sck --thread_mode pool

# One server
mkdir -p /dev/shm/xpn_ckpt /dev/shm/xpn/ckpt && rm -r /dev/shm/xpn_ckpt /dev/shm/xpn && mkdir -p /dev/shm/xpn_ckpt /dev/shm/xpn/ckpt && xpn_server --server_type fabric --thread_mode pool

```

### 4 Launch client
```bash


mana_launch --verbose --with-plugin $HOME/dariomnz/bin/xpn/lib/lfi_dmtcp.so:$HOME/dariomnz/bin/xpn/lib/xpn_dmtcp.so --ckptdir /tmp/expand/xpn/ckpt /home/tester005/dariomnz/bin/ior/bin/ior -w -r -o /tmp/expand/xpn/unito_ior_xpn_test.txt -t 512k -b 10m -s 1 -i 3 -d 2


xpn_ckpt /home/tester005/dariomnz/bin/ior/bin/ior -w -r -o /tmp/expand/xpn/unito_ior_xpn_test.txt -t 512k -b 10m -s 1 -i 3 -d 2
```

### 5 Checkpoint client
```bash
# Checkpoint and kill
mana_status -kc
# Checkpoint
mana_status -c
```

### 6 Cargar ficheros
```bash
rm -r $(pwd)/xpn_ckpt && mkdir $(pwd)/xpn_ckpt && xpn_server_flush $(pwd)/xpn_ckpt

mana_restart --restartdir $(pwd)/xpn_ckpt/ckpt
```