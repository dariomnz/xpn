#include <fcntl.h>
#include <mpi.h>
#include <sys/stat.h>
#include <unistd.h>

#include <iostream>
#include <vector>

#include "xpn.h"

#define ONE_MB (1024 * 1024)

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);

    int world_size, world_rank;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    // 1. Check arguments
    if (argc < 2) {
        if (world_rank == 0) {
            std::cerr << "Usage: mpirun -np <N> " << argv[0] << " <filename>" << std::endl;
        }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    if (xpn_init() < 0) {
        std::cerr << "xpn_init failed" << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    const char* filename = argv[1];

    // Calculate offset: Each process writes 1MB after the previous one
    off_t my_offset = static_cast<off_t>(world_rank) * ONE_MB;

    // 2. Prepare data buffer (fill with a unique pattern per process)
    std::vector<char> write_buf(ONE_MB, static_cast<char>('A' + (world_rank % 26)));
    std::vector<char> read_buf(ONE_MB, 0);

    // 3. Open file (Only rank 0 creates/truncates, others just open for writing)
    int fd;
    if (world_rank == 0) {
        fd = xpn_open(filename, O_RDWR | O_CREAT | O_TRUNC, 0644);
    } else {
        // Wait for rank 0 to create the file to avoid race conditions on open
        MPI_Barrier(MPI_COMM_WORLD);
        fd = xpn_open(filename, O_RDWR);
    }

    if (fd < 0) {
        perror("Error opening file");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // Sync everyone before starting the workload
    if (world_rank == 0)
        MPI_Barrier(MPI_COMM_WORLD);
    else
        MPI_Barrier(MPI_COMM_WORLD);

    // --- WRITE PHASE ---
    ssize_t bytes_written = xpn_pwrite(fd, write_buf.data(), ONE_MB, my_offset);
    if (bytes_written != ONE_MB) {
        std::cerr << "[Rank " << world_rank << "] Write error!" << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // Ensure all writes are finished and flushed to disk
    // xpn_fsync(fd);
    MPI_Barrier(MPI_COMM_WORLD);

    // --- READ & VERIFY PHASE ---
    ssize_t bytes_read = xpn_pread(fd, read_buf.data(), ONE_MB, my_offset);
    if (bytes_read != ONE_MB) {
        std::cerr << "[Rank " << world_rank << "] Read error!" << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // Integrity Check
    for (int i = 0; i < ONE_MB; ++i) {
        if (read_buf[i] != write_buf[i]) {
            std::cerr << "[Rank " << world_rank << "] Data corruption at offset " << my_offset + i << "!" << std::endl;
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }

    // --- CLEANUP ---
    xpn_close(fd);

    if (world_rank == 0) {
        std::cout << "SUCCESS: All processes verified 1MB (" << (world_size * ONE_MB) / (1024.0 * 1024.0)
                  << " MB total)." << std::endl;
    }

    if (xpn_destroy() < 0) {
        std::cerr << "xpn_init failed" << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    MPI_Finalize();
    return 0;
}