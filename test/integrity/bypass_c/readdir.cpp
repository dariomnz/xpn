#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <iostream>
#include <string>
#include <vector>

// Function to check if a directory exists
bool dir_exists(const std::string& path) {
    struct stat info;
    if (stat(path.c_str(), &info) != 0) {
        return false;
    }
    return (info.st_mode & S_IFDIR) != 0;
}

std::string indent(size_t level) { return std::string(level * 2, ' '); }

int count_elems_in_dir(const std::string& path) {
    int ret = 0;
    DIR* dirp = opendir(path.c_str());
    if (dirp == nullptr) {
        std::cerr << "Error: Could not open directory " << path << std::endl;
        exit(EXIT_FAILURE);
    }
    struct dirent* dp;
    while ((dp = readdir(dirp)) != nullptr) {
        std::string name = dp->d_name;
        if (name == "." || name == "..") {
            continue;
        }
        ret += 1;
    }
    closedir(dirp);
    return ret;
}

void run_test(int thread = 0) {
    int elems = 0;
    int elems_expected = 0;
    int files_in_base = 4;
    int files_in_sub = 3;
    std::string base_dir = "/tmp/expand/xpn/test_dir" + std::to_string(thread);
    std::string sub_dir = base_dir + "/subdir";

    // 1. Create the main directory
    if (mkdir(base_dir.c_str(), 0755) != 0) {
        perror("Error creating main directory");
        std::exit(EXIT_FAILURE);
    }
    if (!dir_exists(base_dir)) {
        std::cerr << "Error " << base_dir << " doesn't exist" << std::endl;
        std::exit(EXIT_FAILURE);
    }

    // 2. Create files in the main directory
    for (int i = 0; i < files_in_base; i++) {
        std::string filename = base_dir + "/" + std::to_string(i) + ".txt";
        int file = creat(filename.c_str(), S_IRUSR | S_IWUSR);
        if (file < 0) {
            perror("Error creating file in base directory");
            std::exit(EXIT_FAILURE);
        }
        close(file);
    }

    // 3. Create the subdirectory
    if (mkdir(sub_dir.c_str(), 0755) != 0) {
        perror("Error creating subdirectory");
        std::exit(EXIT_FAILURE);
    }
    if (!dir_exists(sub_dir)) {
        std::cerr << "Error " << sub_dir << " doesn't exist" << std::endl;
        std::exit(EXIT_FAILURE);
    }

    // 4. Create files in the subdirectory
    for (int i = 0; i < files_in_sub; i++) {
        std::string filename = sub_dir + "/" + std::to_string(i) + ".txt";
        int file = creat(filename.c_str(), S_IRUSR | S_IWUSR);
        if (file < 0) {
            perror("Error creating file in base directory");
            std::exit(EXIT_FAILURE);
        }
        close(file);
    }

    // 5. Check the creation of the files in the main directory
    elems_expected = files_in_base + 1;
    elems = count_elems_in_dir(base_dir);
    if (elems != elems_expected) {
        std::cerr << "Error reading content '" << base_dir << "', expected " << elems_expected << " actual " << elems
                  << std::endl;
        std::exit(EXIT_FAILURE);
    }

    // 6. Check the creation of the files in the subdirectory
    elems_expected = files_in_sub;
    elems = count_elems_in_dir(sub_dir);
    if (elems != elems_expected) {
        std::cerr << "Error reading content '" << sub_dir << "', expected " << elems_expected << " actual " << elems
                  << std::endl;
        std::exit(EXIT_FAILURE);
    }

    // 7. Remove the subdirectory files
    for (int i = 0; i < files_in_sub; i++) {
        std::string filename = sub_dir + "/" + std::to_string(i) + ".txt";
        if (unlink(filename.c_str()) < 0) {
            perror("Error unlink file in base directory");
            std::exit(EXIT_FAILURE);
        }
    }
    // 8. Check the elimination of the files in the subdirectory
    elems_expected = 0;
    elems = count_elems_in_dir(sub_dir);
    if (elems != elems_expected) {
        std::cerr << "Error reading content '" << sub_dir << "', expected " << elems_expected << " actual " << elems
                  << std::endl;
        std::exit(EXIT_FAILURE);
    }
    // 9. Remove the subdirectory
    if (rmdir(sub_dir.c_str()) != 0) {
        perror("Error removing subdirectory");
        std::exit(EXIT_FAILURE);
    }
    if (dir_exists(sub_dir)) {
        std::cerr << "Error " << sub_dir << " exist after rmdir" << std::endl;
        std::exit(EXIT_FAILURE);
    }

    // 10. Check content of the main directory
    elems_expected = files_in_base;
    elems = count_elems_in_dir(base_dir);
    if (elems != elems_expected) {
        std::cerr << "Error reading content, expected " << elems_expected << " actual " << elems << std::endl;
        std::exit(EXIT_FAILURE);
    }

    // 11. Remove the files in the main directory
    for (int i = 0; i < files_in_base; i++) {
        std::string filename = base_dir + "/" + std::to_string(i) + ".txt";
        if (unlink(filename.c_str()) < 0) {
            perror("Error unlink file in base directory");
            std::exit(EXIT_FAILURE);
        }
    }
    // 12. Check the elimination of the files in the main directiory
    elems_expected = 0;
    elems = count_elems_in_dir(base_dir);
    if (elems != elems_expected) {
        std::cerr << "Error reading content, expected " << elems_expected << " actual " << elems << std::endl;
        std::exit(EXIT_FAILURE);
    }
    // 13. Remove the main directory
    if (rmdir(base_dir.c_str()) != 0) {
        perror("Error removing main directory");
        std::exit(EXIT_FAILURE);
    }
    if (dir_exists(base_dir)) {
        std::cerr << "Error " << base_dir << " exist after rmdir" << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

int main() {
    run_test();
}