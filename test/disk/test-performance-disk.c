#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/time.h>

#define CHUNK_SIZE (1024ULL * 1024ULL) // 1MB

typedef struct {
    int thread_id;
    int num_threads;
    int fd;
    size_t total_bytes_to_process;
    double write_time;
    double read_time;
} thread_data_t;

pthread_barrier_t barrier;

double get_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)tv.tv_usec / 1000000.0;
}

void* benchmark_thread(void* arg) {
    thread_data_t *data = (thread_data_t*)arg;
    char *buffer = malloc(CHUNK_SIZE);
    memset(buffer, 'A' + (data->thread_id % 26), CHUNK_SIZE);

    size_t bytes_per_thread = data->total_bytes_to_process / data->num_threads;
    size_t start_offset = data->thread_id * bytes_per_thread;

    pthread_barrier_wait(&barrier);
    double start_w = get_time();

    for (size_t off = 0; off < bytes_per_thread; off += CHUNK_SIZE) {
        if (pwrite(data->fd, buffer, CHUNK_SIZE, start_offset + off) == -1) break;
    }
    
    data->write_time = get_time() - start_w;

    // --- TEST DE LECTURA ---
    pthread_barrier_wait(&barrier);
    double start_r = get_time();

    for (size_t off = 0; off < bytes_per_thread; off += CHUNK_SIZE) {
        if (pread(data->fd, buffer, CHUNK_SIZE, start_offset + off) == -1) break;
    }

    data->read_time = get_time() - start_r;

    free(buffer);
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc < 4) {
        fprintf(stderr, "Uso: %s <num_threads> <total_GB> <path_to_file>\n", argv[0]);
        return 1;
    }

    int num_threads = atoi(argv[1]);
    double total_gb = atof(argv[2]);
    char *filepath = argv[3];
    size_t total_bytes = (size_t)(total_gb * 1024 * 1024 * 1024);

    int fd = open(filepath, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd == -1) {
        perror("Error al abrir el archivo");
        return 1;
    }

    pthread_t threads[num_threads];
    thread_data_t t_data[num_threads];
    pthread_barrier_init(&barrier, NULL, num_threads);

    for (int i = 0; i < num_threads; i++) {
        t_data[i].thread_id = i;
        t_data[i].num_threads = num_threads;
        t_data[i].fd = fd;
        t_data[i].total_bytes_to_process = total_bytes;
        pthread_create(&threads[i], NULL, benchmark_thread, &t_data[i]);
    }

    double max_w_time = 0, max_r_time = 0;
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
        if (t_data[i].write_time > max_w_time) max_w_time = t_data[i].write_time;
        if (t_data[i].read_time > max_r_time) max_r_time = t_data[i].read_time;
    }

    printf("\n========================================\n");
    printf(" Threaded Shared-File I/O Benchmark\n");
    printf("========================================\n");
    printf("Target Directory:   %s\n", filepath);
    printf("Number of threads:  %d\n", num_threads);
    printf("Total Data:         %.2f GB\n", total_gb);
    printf("----------------------------------------\n");
    printf("Write Speed:        %.2f MB/s (Time: %.2f s)\n", (total_gb * 1024) / max_w_time, max_w_time);
    printf("Read Speed:         %.2f MB/s (Time: %.2f s)\n", (total_gb * 1024) / max_r_time, max_r_time);
    printf("========================================\n");

    close(fd);
    pthread_barrier_destroy(&barrier);
    unlink(filepath);
    return 0;
}