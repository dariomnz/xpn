#include <chrono>
#include <iostream>
#include <ratio>
#include <thread>

#include "xpn.h"

int main() {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    auto ret_init = xpn_init();
    std::cout << "xpn_init() = " << ret_init << std::endl;

    std::cout << "sleeping 5 seconds" << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(5));

    auto ret_destroy = xpn_destroy();
    std::cout << "xpn_destroy() = " << ret_destroy << std::endl;
}