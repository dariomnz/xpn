#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <mpi.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define MAX_BUFFER_SIZE (64 * 1024 * 1024)
#define INITIAL_SIZE    (4 * 1024 * 1024)

volatile sig_atomic_t signal_stop = 0;
uint64_t test_size_global = INITIAL_SIZE;
pthread_mutex_t size_mutex = PTHREAD_MUTEX_INITIALIZER;

void signal_handler() { signal_stop = 1; }

void *thread_read_stdin() {
    char line[256];
    while (!signal_stop) {
        if (fgets(line, sizeof(line), stdin) == NULL) {
            signal_stop = 1;
            break;
        }
        pthread_mutex_lock(&size_mutex);
        if (strncmp(line, "up", 2) == 0) {
            if (test_size_global * 2 <= MAX_BUFFER_SIZE) test_size_global *= 2;
        } else if (strncmp(line, "down", 4) == 0) {
            if (test_size_global / 2 >= 1) test_size_global /= 2;
        }
        pthread_mutex_unlock(&size_mutex);
    }
    return NULL;
}

double get_time_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1.0e9 + (double)ts.tv_nsec;
}


void update_avg(double* target, double current_value) {
    const double alpha = 0.2;
    if (current_value <= 0) return;
    if ((*target) < 0) {
        (*target) = current_value;
    } else {
        (*target) = (current_value * alpha) + ((*target) * (1.0 - alpha));
    }
}

int main(int argc, char *argv[]) {
    int rank, size;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (argc < 2) {
        if (rank == 0) printf("Usage: %s <directory path>\n", argv[0]);
        MPI_Finalize();
        return 1;
    }

    char filepath[PATH_MAX];
    snprintf(filepath, sizeof(filepath), "%s/test_io_rank_%d.tmp", argv[1], rank);

    int fd = open(filepath, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        fprintf(stderr, "[Rank %d] Error opening file %s: %s\n", rank, filepath, strerror(errno));
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    signal(SIGINT, signal_handler);
    if (rank == 0) {
        pthread_t input_thread;
        pthread_create(&input_thread, NULL, thread_read_stdin, NULL);
        pthread_detach(input_thread);
        printf("Running on %d ranks. Commands: 'up' (double), 'down' (half)\n", size);
        printf("%-12s | %-12s | %-15s | %-12s | %-15s\n", "BUFF_SIZE", "WRITE COUNT",
               "WRITE BW (MB/s)", "READ COUNT", "READ BW (MB/s)");
        printf("----------------------------------------------------------------------\n");
    }

    unsigned char *buffer = malloc(MAX_BUFFER_SIZE);
    for (size_t i = 0; i < MAX_BUFFER_SIZE; i++) {
        if (i % 8 == 0) {
            buffer[i] = rand();
        } else {
            buffer[i] = i;
        }
    }

    bool is_write_phase = true;
    double mbps_avg_r = -1;
    double mbps_avg_w = -1;

    while (!signal_stop) {
        uint64_t current_size;

        if (rank == 0) {
            pthread_mutex_lock(&size_mutex);
            current_size = test_size_global;
            pthread_mutex_unlock(&size_mutex);
        }
        MPI_Bcast(&current_size, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);

        uint64_t local_bytes = 0;
        int64_t local_count = 0;

        MPI_Barrier(MPI_COMM_WORLD);
        double start_ns = get_time_ns();
        double end_interval = start_ns + 1.0e9;

        while (get_time_ns() < end_interval && !signal_stop) {
            lseek(fd, 0, SEEK_SET);
            if (is_write_phase) {
                if (write(fd, buffer, current_size) < 0) break;
            } else {
                if (read(fd, buffer, current_size) < 0) break;
            }
            local_bytes += current_size;
            local_count++;
        }

        double actual_end_ns = get_time_ns();
        double elapsed_s = (actual_end_ns - start_ns) / 1.0e9;

        // Reducción de métricas
        uint64_t total_bytes = 0;
        int64_t total_count = 0;
        MPI_Reduce(&local_bytes, &total_bytes, 1, MPI_UINT64_T, MPI_SUM, 0, MPI_COMM_WORLD);
        MPI_Reduce(&local_count, &total_count, 1, MPI_INT64_T, MPI_SUM, 0, MPI_COMM_WORLD);

        if (rank == 0) {
            double mbps = (total_bytes / (1024.0 * 1024.0)) / elapsed_s;
            if (is_write_phase) {
                printf("%-10lu B | %-12ld | %-15.2f", current_size, total_count, mbps);
                update_avg(&mbps_avg_w, mbps);
            } else {
                printf(" | %-12ld | %-15.2f\n", total_count, mbps);
                update_avg(&mbps_avg_r, mbps);
                printf("%-12s | %-12s | %-15.2f | %-12s | %-15.2f\n", "Averange", "", mbps_avg_w, "", mbps_avg_r);
            }
            fflush(stdout);
        }

        is_write_phase = !is_write_phase;
        MPI_Barrier(MPI_COMM_WORLD);
    }

    free(buffer);
    close(fd);
    unlink(filepath);
    MPI_Finalize();
    return 0;
}