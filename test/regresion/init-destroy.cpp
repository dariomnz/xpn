#include <unistd.h>

#include "base_cpp/debug.hpp"
#include "setup.hpp"
#include "xpn.h"

int main() {
    std::string tmp_dir = "/tmp/" + std::to_string(::getpid());
    auto cleanup_tmp_dir = setup::create_empty_dir(tmp_dir);
    auto cleanup1 = setup::create_empty_dir(tmp_dir + "/xpn");
    XPN::xpn_conf::partition part;
    part.server_urls = {"file://localhost/" + tmp_dir + "/xpn"};
    auto cleanup2 = setup::create_xpn_conf(tmp_dir + "/xpn.conf", part);
    {
        XPN_scope xpn;
    }
}