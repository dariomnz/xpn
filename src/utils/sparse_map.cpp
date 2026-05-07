#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include "base_cpp/debug.hpp"
#include "lz4.h"

// Configuración por defecto
static constexpr uint32_t RAW_HEADER_SIZE = 8192;
static constexpr uint32_t LOGICAL_BLOCK_SIZE = 512 * 1024;
static constexpr uint32_t META_SIZE = sizeof(uint32_t) * 2;

static inline const uint32_t MAX_COMP_SIZE = LZ4_COMPRESSBOUND(LOGICAL_BLOCK_SIZE);
static constexpr uint32_t ALIGNMENT = 4096;
static constexpr uint32_t PHYSICAL_BLOCK_SIZE = (META_SIZE + MAX_COMP_SIZE + (ALIGNMENT - 1)) & ~(ALIGNMENT - 1);

int count_data_units(int fd, off_t start, off_t end, size_t sub_size) {
    int count = 0;
    for (off_t offset = start; offset < end; offset += sub_size) {
        off_t current_sub_end = std::min(offset + (off_t)sub_size, end);
        off_t next_data = lseek(fd, offset, SEEK_DATA);
        if (next_data != -1 && next_data < current_sub_end) {
            count++;
        }
    }
    return count;
}

void draw_range(int fd, off_t start, off_t end, size_t sub_size) {
    std::cout << "|";
    for (off_t offset = start; offset < end; offset += sub_size) {
        off_t current_sub_end = std::min(offset + (off_t)sub_size, end);
        off_t next_data = lseek(fd, offset, SEEK_DATA);

        if (next_data != -1 && next_data < current_sub_end) {
            std::cout << "█";
        } else {
            std::cout << " ";
        }
    }
    std::cout << "|";
}

// void draw_range(int fd, off_t start, off_t end, size_t sub_size, int& data_count, int& empty_count) {
//     std::cout << "|";
//     for (off_t offset = start; offset < end; offset += sub_size) {
//         off_t current_sub_end = offset + sub_size;
//         if (current_sub_end > end) current_sub_end = end;

//         // Comprobamos si hay datos en este sub-bloque
//         off_t next_data = lseek(fd, offset, SEEK_DATA);

//         if (next_data != -1 && next_data < current_sub_end) {
//             std::cout << "█";
//             data_count++;
//         } else {
//             std::cout << " ";
//             empty_count++;
//         }
//     }
//     std::cout << "|";
// }

void draw_sparse_map(const char* filename) {
    int fd = open(filename, O_RDONLY);
    if (fd < 0) {
        perror("Error opening file");
        return;
    }

    struct stat st;
    fstat(fd, &st);
    size_t total_size = st.st_size;
    size_t sub_block_sz = st.st_blksize;
    if (total_size == 0) {
        std::cout << "File is empty.\n";
        close(fd);
        return;
    }

    int data_visual_blocks = 0;
    int empty_visual_blocks = 0;

    std::cout << "\nVisual Map of " << filename << " (Sub-block: " << sub_block_sz / 1024 << " KB)\n";
    std::cout << std::string(60, '-') << "\n";

    // --- 1. Header (8 KB) ---
    off_t h_start = 0;
    off_t h_end = RAW_HEADER_SIZE;

    // A. Escaneo previo para el porcentaje del Header
    int h_filled = count_data_units(fd, h_start, h_end, sub_block_sz);
    int h_total_units = (h_end - h_start + sub_block_sz - 1) / sub_block_sz;
    double h_pct = (h_total_units > 0) ? ((double)h_filled / h_total_units) * 100.0 : 0;

    // Actualizamos estadísticas globales
    data_visual_blocks += h_filled;
    empty_visual_blocks += (h_total_units - h_filled);

    // B. Imprimir con el mismo formato que los bloques para que quede alineado
    std::cout << std::left << std::setw(6) << "Header"
              << " [" << std::fixed << std::setprecision(1) << std::setw(5) << std::right << h_pct << "%] "
              << "(" << std::setw(6) << XPN::format_bytes(RAW_HEADER_SIZE) << ") ";

    draw_range(fd, h_start, h_end, sub_block_sz);
    std::cout << "\n";

    // --- 2. Bloques (512 KB cada uno) ---
    off_t current_offset = RAW_HEADER_SIZE;
    int block_idx = 0;

    while (current_offset < (off_t)total_size) {
        off_t b_end = std::min(current_offset + (off_t)PHYSICAL_BLOCK_SIZE, (off_t)total_size);

        // A. Escaneo previo para el porcentaje
        int filled_units = count_data_units(fd, current_offset, b_end, sub_block_sz);
        int total_units_in_block = (b_end - current_offset + sub_block_sz - 1) / sub_block_sz;
        double block_pct = (total_units_in_block > 0) ? ((double)filled_units / total_units_in_block) * 100.0 : 0;

        // Actualizamos estadísticas globales
        data_visual_blocks += filled_units;
        empty_visual_blocks += (total_units_in_block - filled_units);

        // B. Imprimir Prefijo (incluyendo porcentaje precargado)
        std::cout << "Block " << std::setw(4) << std::left << block_idx++ << " [" << std::fixed << std::setprecision(1)
                  << std::setw(5) << std::right << block_pct << "%] "
                  << "(" << std::setw(6) << XPN::format_bytes(b_end - current_offset) << ") ";

        // C. Dibujar el mapa visual
        draw_range(fd, current_offset, b_end, sub_block_sz);
        std::cout << "\n";

        current_offset += PHYSICAL_BLOCK_SIZE;
    }

    // --- 3. Estadísticas ---
    long total_physical_bytes = (long)st.st_blocks * 512;
    double data_pct = (total_size > 0) ? ((double)total_physical_bytes / total_size) * 100.0 : 0;

    std::cout << "\n--- Statistics ---\n";
    std::cout << std::left << std::setw(25) << "Total Logical Size:" << total_size << " bytes ("
              << XPN::format_bytes(total_size) << ")\n";
    std::cout << std::left << std::setw(25) << "Physical Size (Disk):" << total_physical_bytes << " bytes ("
              << XPN::format_bytes(total_physical_bytes) << ")\n";
    std::cout << std::left << std::setw(25) << "FS Block size:" << st.st_blksize << "\n";
    std::cout << std::left << std::setw(25) << "Visual Units (Data):" << data_visual_blocks << " [█]\n";
    std::cout << std::left << std::setw(25) << "Visual Units (Hole):" << empty_visual_blocks << " [ ]\n";
    std::cout << std::left << std::setw(25) << "Actual Occupancy:" << std::fixed << std::setprecision(2) << data_pct
              << "%\n";

    close(fd);
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <filename> [sub_block_kb]\n";
        return 1;
    }

    draw_sparse_map(argv[1]);
    return 0;
}