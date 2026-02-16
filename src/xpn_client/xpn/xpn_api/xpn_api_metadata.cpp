
/*
 *  Copyright 2020-2024 Felix Garcia Carballeira, Diego Camarmas Alonso, Alejandro Calderon Mateos, Dario Muñoz Muñoz
 *
 *  This file is part of Expand.
 *
 *  Expand is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Lesser General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Expand is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with Expand.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

#include "base_cpp/fixed_task_queue.hpp"
#include "xpn/xpn_api.hpp"

#include <vector>

namespace XPN
{
    int xpn_api::read_metadata(xpn_metadata &mdata)
    {
        XPN_DEBUG_BEGIN_CUSTOM(mdata.m_file.m_path);
        int res = 0;
        int master = mdata.master_file();
        XPN_DEBUG("Read metadata from serv "<<master);
        res = mdata.m_file.m_part.m_data_serv[master]->nfi_read_mdata(mdata.m_file.m_path, mdata);

        XPN_DEBUG(mdata);

        XPN_DEBUG_END_CUSTOM(mdata.m_file.m_path);
        return res;
    }

    int xpn_api::write_metadata(xpn_metadata &mdata, bool only_file_size)
    {
        XPN_DEBUG_BEGIN_CUSTOM(mdata.m_file.m_path<<", "<<only_file_size);
        int res = 0;
        if (only_file_size){
            XPN_DEBUG("New file_size: "<<mdata.m_data.file_size);
        }else{
            XPN_DEBUG(mdata);
        }

        int server = xpn_path::hash(mdata.m_file.m_path, mdata.m_file.m_part.m_data_serv.size(), true);

        auto result_handler = [&](const WorkerResult& r) {
            if (r.result < 0) {
                res = r.result;
                errno = r.errorno;
            }
            return true; // Continue
        };
        FixedTaskQueue tasks(*m_worker, result_handler);
        for (int i = 0; i < mdata.m_file.m_part.m_replication_level+1; i++)
        {
            server = (server+i) % mdata.m_file.m_part.m_data_serv.size();
            if (mdata.m_file.m_part.m_data_serv[server]->m_error != -1){
                XPN_DEBUG("Write metadata to serv "<<server); 
                res = mdata.m_file.initialize_vfh(server);
                if (res < 0){
                    continue;
                }
                tasks.launch([server, &mdata, only_file_size](){
                    int res = mdata.m_file.m_part.m_data_serv[server]->nfi_write_mdata(mdata.m_file.m_path, mdata.m_file.m_data_vfh[server], mdata.m_data, only_file_size);
                    return WorkerResult(res);
                });
            }
        }
        tasks.wait_remaining();

        XPN_DEBUG_END_CUSTOM(mdata.m_file.m_path<<", "<<only_file_size);
        return res;
    }
} // namespace XPN
