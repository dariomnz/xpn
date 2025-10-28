
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

/* ... Include / Inclusion ........................................... */
// #define DEBUG
#include <pthread.h>
#include <stdio.h>

#include "config.h"
#include "dmtcp.h"
#include "lower-half-api.h"
#include "lfi.h"
#include "lfi_async.h"
#include "lfi_coll.h"

/* ... Const / Const ................................................. */

#ifdef DEBUG
#define debug_info(...)                             \
    {                                               \
        NEXT_FNC(printf)("[%ld] ", pthread_self()); \
        NEXT_FNC(printf)(__VA_ARGS__);              \
        NEXT_FNC(printf)("\n");                     \
        NEXT_FNC(fflush)(stdout);                    \
    }
#else
#define debug_info(...)
#endif

#ifndef NEXT_FUNC
#define NEXT_FUNC(func) ({ (__typeof__(&lfi_##func))pdlsym(lfi_Fnc_##func); })
#endif  // ifndef NEXT_FUNC
// #ifndef NEXT_FUNC
// #define NEXT_FUNC1(func) NEXT_FNC(lfi_##func)
// #define NEXT_FUNC(func) NEXT_FUNC1(func)
// #endif // ifndef NEXT_FUNC

/* ... Global variables / Variables globales ........................ */

/* ... Auxiliar functions / Funciones auxiliares ......................................... */

/* ... Functions / Funciones ......................................... */

// lfi.h
extern "C" const char *lfi_strerror(int error) {
    const char *ret;
    debug_info("[BYPASS] >> lfi_strerror(%d)", error);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(strerror)(error);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_strerror(%d) -> %s", error, ret);
    return ret;
}

extern "C" int lfi_server_create(const char *serv_addr, int *port) {
    int ret;
    debug_info("[BYPASS] >> lfi_server_create(%s, %p)", serv_addr, port);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(server_create)(serv_addr, port);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_server_create(%s, %p) -> %d", serv_addr, port, ret);
    return ret;
}

extern "C" int lfi_server_accept(int id) {
    int ret;
    debug_info("[BYPASS] >> lfi_server_accept(%d)", id);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(server_accept)(id);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_server_accept(%d) -> %d", id, ret);
    return ret;
}

extern "C" int lfi_server_close(int id) {
    int ret;
    debug_info("[BYPASS] >> lfi_server_close(%d)", id);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(server_close)(id);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_server_close(%d) -> %d", id, ret);
    return ret;
}

extern "C" int lfi_client_create(const char *serv_addr, int port) {
    int ret;
    debug_info("[BYPASS] >> lfi_client_create(%s, %d)", serv_addr, port);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(client_create)(serv_addr, port);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_client_create(%s, %d) -> %d", serv_addr, port, ret);
    return ret;
}

extern "C" int lfi_client_close(int id) {
    int ret;
    debug_info("[BYPASS] >> lfi_client_close(%d)", id);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(client_close)(id);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_client_close(%d) -> %d", id, ret);
    return ret;
}

extern "C" ssize_t lfi_send(int id, const void *data, size_t size) {
    ssize_t ret;
    debug_info("[BYPASS] >> lfi_send(%d, %p, %ld)", id, data, size);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(send)(id, data, size);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_send(%d, %p, %ld) -> %ld", id, data, size, ret);
    return ret;
}

extern "C" ssize_t lfi_tsend(int id, const void *data, size_t size, int tag) {
    ssize_t ret;
    debug_info("[BYPASS] >> lfi_tsend(%d, %p, %ld, %d)", id, data, size, tag);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(tsend)(id, data, size, tag);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_tsend(%d, %p, %ld, %d) -> %ld", id, data, size, tag, ret);
    return ret;
}

extern "C" ssize_t lfi_recv(int id, void *data, size_t size) {
    ssize_t ret;
    debug_info("[BYPASS] >> lfi_recv(%d, %p, %ld)", id, data, size);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(recv)(id, data, size);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_recv(%d, %p, %ld) -> %ld", id, data, size, ret);
    return ret;
}

extern "C" ssize_t lfi_trecv(int id, void *data, size_t size, int tag) {
    ssize_t ret;
    debug_info("[BYPASS] >> lfi_trecv(%d, %p, %ld, %d)", id, data, size, tag);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(trecv)(id, data, size, tag);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_trecv(%d, %p, %ld, %d) -> %ld", id, data, size, tag, ret);
    return ret;
}

// lfi_async.h
extern "C" lfi_request *lfi_request_create(int id) {
    lfi_request *ret;
    debug_info("[BYPASS] >> lfi_request_create(%d)", id);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(request_create)(id);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_request_create(%d) -> %p", id, ret);
    return ret;
}

extern "C" void lfi_request_free(lfi_request *request) {
    debug_info("[BYPASS] >> lfi_request_free(%p)", request);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    NEXT_FUNC(request_free)(request);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_request_free(%p)", request);
}

extern "C" bool lfi_request_completed(lfi_request *request) {
    bool ret;
    debug_info("[BYPASS] >> lfi_request_completed(%p)", request);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(request_completed)(request);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_request_completed(%p) -> %d", request, ret);
    return ret;
}

extern "C" ssize_t lfi_request_size(lfi_request *request) {
    ssize_t ret;
    debug_info("[BYPASS] >> lfi_request_size(%p)", request);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(request_size)(request);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_request_size(%p) -> %ld", request, ret);
    return ret;
}

extern "C" ssize_t lfi_request_source(lfi_request *request) {
    ssize_t ret;
    debug_info("[BYPASS] >> lfi_request_source(%p)", request);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(request_source)(request);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_request_source(%p) -> %ld", request, ret);
    return ret;
}

extern "C" ssize_t lfi_request_error(lfi_request *request) {
    ssize_t ret;
    debug_info("[BYPASS] >> lfi_request_error(%p)", request);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(request_error)(request);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_request_error(%p) -> %ld", request, ret);
    return ret;
}

extern "C" ssize_t lfi_send_async(lfi_request *request, const void *data, size_t size) {
    ssize_t ret;
    debug_info("[BYPASS] >> lfi_send_async(%p, %p, %ld)", request, data, size);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(send_async)(request, data, size);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_send_async(%p, %p, %ld) -> %ld", request, data, size, ret);
    return ret;
}

extern "C" ssize_t lfi_tsend_async(lfi_request *request, const void *data, size_t size, int tag) {
    ssize_t ret;
    debug_info("[BYPASS] >> lfi_tsend_async(%p, %p, %ld, %d)", request, data, size, tag);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(tsend_async)(request, data, size, tag);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_tsend_async(%p, %p, %ld, %d) -> %ld", request, data, size, tag, ret);
    return ret;
}

extern "C" ssize_t lfi_recv_async(lfi_request *request, void *data, size_t size) {
    ssize_t ret;
    debug_info("[BYPASS] >> lfi_recv_async(%p, %p, %ld)", request, data, size);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(recv_async)(request, data, size);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_recv_async(%p, %p, %ld) -> %ld", request, data, size, ret);
    return ret;
}

extern "C" ssize_t lfi_trecv_async(lfi_request *request, void *data, size_t size, int tag) {
    ssize_t ret;
    debug_info("[BYPASS] >> lfi_trecv_async(%p, %p, %ld, %d)", request, data, size, tag);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(trecv_async)(request, data, size, tag);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_trecv_async(%p, %p, %ld, %d) -> %ld", request, data, size, tag, ret);
    return ret;
}

extern "C" ssize_t lfi_wait(lfi_request *request) {
    ssize_t ret;
    debug_info("[BYPASS] >> lfi_wait(%p)", request);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(wait)(request);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_wait(%p) -> %ld", request, ret);
    return ret;
}

extern "C" ssize_t lfi_wait_any(lfi_request *requests[], size_t size) {
    ssize_t ret;
    debug_info("[BYPASS] >> lfi_wait_any(%p, %ld)", requests, size);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(wait_any)(requests, size);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_wait_any(%p, %ld) -> %ld", requests, size, ret);
    return ret;
}

extern "C" ssize_t lfi_wait_all(lfi_request *requests[], size_t size) {
    ssize_t ret;
    debug_info("[BYPASS] >> lfi_wait_all(%p, %ld)", requests, size);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(wait_all)(requests, size);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_wait_all(%p, %ld) -> %ld", requests, size, ret);
    return ret;
}

extern "C" ssize_t lfi_cancel(lfi_request *request) {
    ssize_t ret;
    debug_info("[BYPASS] >> lfi_cancel(%p)", request);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(cancel)(request);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_cancel(%p) -> %ld", request, ret);
    return ret;
}

// lfi_coll.h
extern "C" int lfi_group_create(const char *hostnames[], size_t n_hosts, lfi_group *out_group) {
    int ret;
    debug_info("[BYPASS] >> lfi_group_create(%p, %ld, %p)", hostnames, n_hosts, out_group);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(group_create)(hostnames, n_hosts, out_group);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_group_create(%p, %ld, %p) -> %d", hostnames, n_hosts, out_group, ret);
    return ret;
}

extern "C" int lfi_group_rank(lfi_group *group, int *rank) {
    int ret;
    debug_info("[BYPASS] >> lfi_group_rank(%p, %d)", group, *rank);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(group_rank)(group, rank);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_group_rank(%p, %d) -> %d", group, *rank, ret);
    return ret;
}

extern "C" int lfi_group_size(lfi_group *group, int *size) {
    int ret;
    debug_info("[BYPASS] >> lfi_group_size(%p, %d)", group, *size);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(group_size)(group, size);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_group_size(%p, %d) -> %d", group, *size, ret);
    return ret;
}

extern "C" int lfi_group_close(lfi_group *group) {
    int ret;
    debug_info("[BYPASS] >> lfi_group_close(%p)", group);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(group_close)(group);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_group_close(%p) -> %d", group, ret);
    return ret;
}

extern "C" int lfi_barrier(lfi_group *group) {
    int ret;
    debug_info("[BYPASS] >> lfi_barrier(%p)", group);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(barrier)(group);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_barrier(%p) -> %d", group, ret);
    return ret;
}

extern "C" int lfi_broadcast(lfi_group *group, int root, void *data, size_t size) {
    int ret;
    debug_info("[BYPASS] >> lfi_broadcast(%p, %d, %p, %ld)", group, root, data, size);
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(broadcast)(group, root, data, size);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_broadcast(%p, %d, %p, %ld) -> %d", group, root, data, size, ret);
    return ret;
}

extern "C" int lfi_allreduce(lfi_group *group, void *data, enum lfi_op_type_enum type, enum lfi_op_enum op) {
    int ret;
    debug_info("[BYPASS] >> lfi_allreduce(%p, %d, %d, %d)", group, data, static_cast<int>(type), static_cast<int>(op));
    DMTCP_PLUGIN_DISABLE_CKPT();
    JUMP_TO_LOWER_HALF(lh_info->fsaddr);
    ret = NEXT_FUNC(allreduce)(group, data, type, op);
    RETURN_TO_UPPER_HALF();
    DMTCP_PLUGIN_ENABLE_CKPT();
    debug_info("[BYPASS] << lfi_allreduce(%p, %d, %d, %d) -> %d", group, data, static_cast<int>(type), static_cast<int>(op), ret);
    return ret;
}

static void lfi_event_hook([[maybe_unused]] DmtcpEvent_t event, [[maybe_unused]] DmtcpEventData_t *data) {}

DmtcpPluginDescriptor_t lfi_plugin = {DMTCP_PLUGIN_API_VERSION, PACKAGE_VERSION, "lfi",         "lfi",
                                      "lfi@gmail.todo",         "LFI plugin",    lfi_event_hook};

DMTCP_DECL_PLUGIN(lfi_plugin);