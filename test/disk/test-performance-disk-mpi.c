#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#define GIB_SIZE (1024ULL * 1024ULL * 1024ULL)
#define CHUNK_SIZE (1024ULL * 1024ULL) // 1MB buffer

int main(int argc, char *argv[]) {
    int rank, size;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (argc < 2) {
        if (rank == 0) {
            fprintf(stderr, "Usage: mpirun -np <N> %s <target_directory>\n", argv[0]);
        }
        MPI_Finalize();
        return 1;
    }

    char filename[512];
    snprintf(filename, sizeof(filename), "%s/mpi_test_file_%d.bin", argv[1], rank);

    size_t total_bytes = GIB_SIZE*8;
    char *buffer = malloc(CHUNK_SIZE);
    if (!buffer) {
        perror("Failed to allocate buffer");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    memset(buffer, 'X', CHUNK_SIZE);

    MPI_Barrier(MPI_COMM_WORLD); 
    double start_w = MPI_Wtime();
    
    int fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd == -1) {
        fprintf(stderr, "Rank %d: Error opening file %s for writing\n", rank, filename);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    for (size_t written = 0; written < total_bytes; written += CHUNK_SIZE) {
        if (rank == 0 && written % GIB_SIZE == 0) printf("Written %ld\n", written);
        if (write(fd, buffer, CHUNK_SIZE) == -1) break;
    }
    // fsync(fd); // Force flush to physical disk
    close(fd);
    

    double local_w_time = MPI_Wtime() - start_w;

    MPI_Barrier(MPI_COMM_WORLD);
    double start_r = MPI_Wtime();

    fd = open(filename, O_RDONLY);
    if (fd == -1) {
        fprintf(stderr, "Rank %d: Error opening file %s for reading\n", rank, filename);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    for (size_t read_bytes = 0; read_bytes < total_bytes; read_bytes += CHUNK_SIZE) {
        if (rank == 0 && read_bytes % GIB_SIZE == 0) printf("read_bytes %ld\n", read_bytes);
        if (read(fd, buffer, CHUNK_SIZE) == -1) break;
    }
    close(fd);

    double local_r_time = MPI_Wtime() - start_r;

    double max_w_time, max_r_time;
    MPI_Reduce(&local_w_time, &max_w_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&local_r_time, &max_r_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        double total_gb = (double)(total_bytes * size) / (1024 * 1024 * 1024);
        printf("\n========================================\n");
        printf(" MPI Disk I/O Benchmark (POSIX)\n");
        printf("========================================\n");
        printf("Target Directory: %s\n", argv[1]);
        printf("Number of Ranks:  %d\n", size);
        printf("Total Data:       %.2f GB\n", total_gb);
        printf("----------------------------------------\n");
        printf("Write Speed:      %.2f MB/s (Time: %.2f s)\n", (total_gb * 1024) / max_w_time, max_w_time);
        printf("Read Speed:       %.2f MB/s (Time: %.2f s)\n", (total_gb * 1024) / max_r_time, max_r_time);
        printf("========================================\n");
    }

    // Cleanup
    free(buffer);
    unlink(filename); 
    MPI_Finalize();
    return 0;
}