/*
 * Description:
 *     History: yang@haipo.me, 2017/03/16, create
 */

# include "me_config.h"
# include "me_server.h"
# include "me_balance.h"
# include "me_update.h"
# include "me_market.h"
# include "me_trade.h"
# include "me_operlog.h"
# include "me_history.h"

static rpc_svr *svr;
static dict_t *dict_cache;
static nw_timer cache_timer;

struct cache_val {
    double      time;
    json_t      *result;
};

static int reply_json(nw_ses *ses, rpc_pkg *pkg, const json_t *json, bool log)
{
    char *message_data;
    if (settings.debug) {
        message_data = json_dumps(json, JSON_INDENT(4));
    } else {
        message_data = json_dumps(json, 0);
    }
    if (message_data == NULL)
        return -__LINE__;

    if (log)
        log_trace("connection: %s send: %s", nw_sock_human_addr(&ses->peer_addr), message_data);

    rpc_pkg reply;
    memcpy(&reply, pkg, sizeof(reply));
    reply.pkg_type = RPC_PKG_TYPE_REPLY;
    reply.body = message_data;
    reply.body_size = strlen(message_data);
    rpc_send(ses, &reply);
    free(message_data);

    return 0;
}

static int reply_error(nw_ses *ses, rpc_pkg *pkg, int code, const char *message)
{
    json_t *error = json_object();
    json_object_set_new(error, "code", json_integer(code));
    json_object_set_new(error, "message", json_string(message));

    json_t *reply = json_object();
    json_object_set_new(reply, "error", error);
    json_object_set_new(reply, "result", json_null());
    json_object_set_new(reply, "id", json_integer(pkg->req_id));

    int ret = reply_json(ses, pkg, reply, true);
    json_decref(reply);

    return ret;
}

static int reply_error_invalid_argument(nw_ses *ses, rpc_pkg *pkg)
{
    return reply_error(ses, pkg, 1, "invalid argument");
}

static int reply_error_internal_error(nw_ses *ses, rpc_pkg *pkg)
{
    return reply_error(ses, pkg, 2, "internal error");
}

static int reply_error_service_unavailable(nw_ses *ses, rpc_pkg *pkg)
{
    return reply_error(ses, pkg, 3, "service unavailable");
}

static int reply_result(nw_ses *ses, rpc_pkg *pkg, json_t *result, bool log)
{
    json_t *reply = json_object();
    json_object_set_new(reply, "error", json_null());
    json_object_set    (reply, "result", result);
    json_object_set_new(reply, "id", json_integer(pkg->req_id));

    int ret = reply_json(ses, pkg, reply, log);
    json_decref(reply);

    return ret;
}

static int reply_success(nw_ses *ses, rpc_pkg *pkg)
{
    json_t *result = json_object();
    json_object_set_new(result, "status", json_string("success"));

    int ret = reply_result(ses, pkg, result, true);
    json_decref(result);
    return ret;
}

static bool process_cache(nw_ses *ses, rpc_pkg *pkg, sds *cache_key)
{
    sds key = sdsempty();
    key = sdscatprintf(key, "%u", pkg->command);
    key = sdscatlen(key, pkg->body, pkg->body_size);
    dict_entry *entry = dict_find(dict_cache, key);
    if (entry == NULL) {
        *cache_key = key;
        return false;
    }

    struct cache_val *cache = entry->val;
    double now = current_timestamp();
    if ((now - cache->time) > settings.cache_timeout) {
        dict_delete(dict_cache, key);
        *cache_key = key;
        return false;
    }

    reply_result(ses, pkg, cache->result, true);
    sdsfree(key);
    return true;
}

static int add_cache(sds cache_key, json_t *result)
{
    struct cache_val cache;
    cache.time = current_timestamp();
    cache.result = result;
    json_incref(result);
    dict_replace(dict_cache, cache_key, &cache);

    return 0;
}

static int on_cmd_asset_list(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    json_t *result = json_array();
    for (int i = 0; i < settings.asset_num; ++i) {
        json_t *asset = json_object();
        json_object_set_new(asset, "name", json_string(settings.assets[i].name));
        json_object_set_new(asset, "prec", json_integer(settings.assets[i].prec_show));
        json_array_append_new(result, asset);
    }

    int ret = reply_result(ses, pkg, result, false);
    json_decref(result);
    return ret;
}

static int on_cmd_asset_update(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 0)
        return reply_error_invalid_argument(ses, pkg);

    int ret = init_asset_and_market(false);
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "asset update fail: %d", ret);
    }

    init_balance();
    return reply_success(ses, pkg);
}

// envelope.put_envelope (uid, asset, supply, share, type, expire_time)
static int on_cmd_envelope_put(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 6)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // asset
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *asset = json_string_value(json_array_get(params, 1));
    int prec = asset_prec_show(asset);
    if (prec < 0)
        return reply_error_invalid_argument(ses, pkg);

    // supply
    if (!json_is_string(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    const char *supply = json_string_value(json_array_get(params, 2));
    if (atof(supply) < 0.000001)
        return reply_error_invalid_argument(ses, pkg);

    // share
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t share = json_integer_value(json_array_get(params, 3));
    if (share < MIX_ENVELOPE_SHARE || share > MAX_ENVELOPE_SHARE)
        return reply_error_invalid_argument(ses, pkg);

    // type
    if (!json_is_integer(json_array_get(params, 4)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t type = json_integer_value(json_array_get(params, 4));
    if (type != ENVELOPE_TYPE_AVERAGE && type != ENVELOPE_TYPE_RANDOM)
        return reply_error_invalid_argument(ses, pkg);

    // expire_time
    uint32_t expire_time = json_integer_value(json_array_get(params, 5));
    if (expire_time == 0)
        expire_time = 24;

    market_t *market = get_market(settings.markets[0].name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    json_t *result = NULL;
    double create_time = current_timestamp();
    int ret = envelope_put(true, &result, market, user_id, asset, supply, share, type, expire_time, create_time);

    if (ret > 0) {
        json_decref(result);
        if (ret == 2) {
            return reply_error(ses, pkg, 2, "internal error");
        } else if (ret == 3) {
            return reply_error(ses, pkg, 3, "service unavailable");
        } else if (ret == 5) {
            return reply_error(ses, pkg, 5, "service timeout");
        } else if (ret == 10) {
            return reply_error(ses, pkg, 10, "repeat update");
        } else if (ret == 11) {
            return reply_error(ses, pkg, 11, "balance not enough");
        }
    } else if (ret == -1) {
        if (result != NULL) {
            json_decref(result);
	}
        return reply_error(ses, pkg, 9, "connection refused");
    } else if (ret < 0) {
        log_fatal("envelope_put fail: %d", ret);
	if (result != NULL) {
            json_decref(result);
	}
        return reply_error_internal_error(ses, pkg);
    }

    append_operlog_time("envelope_put", params, create_time);
    ret = reply_result(ses, pkg, result, true);
    json_decref(result);
    return ret;
}

// envelope.open_envelope (uid, asset, envelope_id)
static int on_cmd_envelope_open(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 3)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // asset
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *asset = json_string_value(json_array_get(params, 1));
    int prec = asset_prec_show(asset);
    if (prec < 0)
        return reply_error_invalid_argument(ses, pkg);

    // order_id
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t order_id = json_integer_value(json_array_get(params, 2));

    market_t *market = get_market(settings.markets[0].name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    order_t *order = market_get_order(market, order_id);

    if (order == NULL) {
        if (get_envelope_from_db(NULL, order_id) != 0) {
            return reply_error(ses, pkg, 13, "envelope not found");
        } else {
            return reply_error(ses, pkg, 12, "envelope has been finished");
        }
    }

    if (strcmp(order->asset, asset) != 0) {
        return reply_error(ses, pkg, 13, "envelope not found");
    }

    json_t *result = NULL;
    int ret = envelope_open(true, &result, market, user_id, order);

    if (ret > 0) {
        if (ret == 2) {
            return reply_error(ses, pkg, 2, "internal error");
        } else if (ret == 3) {
            return reply_error(ses, pkg, 3, "service unavailable");
        } else if (ret == 5) {
            return reply_error(ses, pkg, 5, "service timeout");
        } else if (ret == 10) {
            return reply_error(ses, pkg, 10, "repeat update");
        } else if (ret == 11) {
            return reply_error(ses, pkg, 11, "balance not enough");
        } else if (ret == 14) {
            return reply_error(ses, pkg, 14, "envelope expired");
        }
    } else if (ret == -1) {
        return reply_error(ses, pkg, 9, "connection refused");
    } else if (ret < 0) {
        log_fatal("envelope_open fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }

    append_operlog("envelope_open", params);
    ret = reply_result(ses, pkg, result, true);
    json_decref(result);
    return ret;
}

// envelope.history (uid, asset, start_time, end_time, offset, limit, role)
static int on_cmd_envelope_history(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 7)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // asset
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *asset = json_string_value(json_array_get(params, 1));

    // start_time, end_time
    uint64_t start_time = json_integer_value(json_array_get(params, 2));
    uint64_t end_time   = json_integer_value(json_array_get(params, 3));
    if (end_time && start_time > end_time)
        return reply_error_invalid_argument(ses, pkg);

    // offset
    if (!json_is_integer(json_array_get(params, 4)))
        return reply_error_invalid_argument(ses, pkg);
    size_t offset = json_integer_value(json_array_get(params, 4));

    // limit
    if (!json_is_integer(json_array_get(params, 5)))
        return reply_error_invalid_argument(ses, pkg);
    size_t limit = json_integer_value(json_array_get(params, 5));
    if (limit > ORDER_LIST_MAX_LEN)
        return reply_error_invalid_argument(ses, pkg);

    // role
    int role = json_integer_value(json_array_get(params, 6));
    if (role != 0 && role != MARKET_ROLE_MAKER && role != MARKET_ROLE_TAKER)
        return reply_error_invalid_argument(ses, pkg);

    json_t *result = json_object();
    int ret = get_user_envelope_history(result, user_id, asset, start_time, end_time, offset, limit, role);
    if (ret != 0)
         return reply_error_internal_error(ses, pkg);

    uint64_t total = get_user_envelope_history_total(user_id, asset, start_time, end_time, role);
    if(total < 0){
	json_decref(result);
        return reply_error_internal_error(ses, pkg);
    }

    json_object_set_new(result, "offset", json_integer(offset));
    json_object_set_new(result, "limit", json_integer(limit));
    json_object_set_new(result, "total", json_integer(total));
    ret = reply_result(ses, pkg, result, false);
    json_decref(result);
    return ret;
}

static int on_cmd_envelope_detail(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 1)
        return reply_error_invalid_argument(ses, pkg);

    // envelope_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t envelope_id = json_integer_value(json_array_get(params, 0));

    market_t *market = get_market(settings.markets[0].name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    order_t *order = market_get_order(market, envelope_id);
    json_t *result = json_object();
    int ret = 0;
    if (order == NULL) {
        ret = get_envelope_from_db(result, envelope_id);
        if (ret == 1) {
            return reply_error(ses, pkg, 13, "envelope not found");
        } else if (ret < 0) {
            return reply_error_internal_error(ses, pkg);
        }
    } else {
        result = get_order_info(order, -1);
    }

    ret = reply_result(ses, pkg, result, false);
    json_decref(result);
    return ret;
}

static int on_cmd_order_query(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 2)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    market_t *market = get_market(settings.markets[0].name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    size_t offset = 0;

    // limit
    if (!json_is_integer(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    size_t limit = json_integer_value(json_array_get(params, 1));
    if (limit > ORDER_LIST_MAX_LEN)
        return reply_error_invalid_argument(ses, pkg);

    json_t *result = json_object();
    json_object_set_new(result, "limit", json_integer(limit));

    json_t *orders = json_array();
    skiplist_t *order_list = market_get_order_list(market, user_id);
    if (order_list == NULL) {
        json_object_set_new(result, "total", json_integer(0));
    } else {
        json_object_set_new(result, "total", json_integer(order_list->len));
        if (offset < order_list->len) {
            skiplist_iter *iter = skiplist_get_iterator(order_list);
            skiplist_node *node;
            for (size_t i = 0; i < offset; i++) {
                if (skiplist_next(iter) == NULL)
                    break;
            }
            size_t index = 0;
            while ((node = skiplist_next(iter)) != NULL && index < limit) {
                index++;
                order_t *order = node->value;
                json_array_append_new(orders, get_order_info(order, -1));
            }
            skiplist_release_iterator(iter);
        }
    }

    json_object_set_new(result, "records", orders);
    int ret = reply_result(ses, pkg, result, false);
    json_decref(result);
    return ret;
}

static int on_cmd_order_book(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 1)
        return reply_error_invalid_argument(ses, pkg);

    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    size_t book = json_integer_value(json_array_get(params, 0));

    json_t *result = json_object();
    int ret = query_envelope_book(result, book);     
    if (ret != 0) {
        log_error("query_envelope_detail fail");
        return reply_error_internal_error(ses, pkg);
    }

    ret = reply_result(ses, pkg, result, false);
    json_decref(result);
    return ret;
}

static int on_cmd_order_cancel(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 1)
        return reply_error_invalid_argument(ses, pkg);

    // order_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t order_id = json_integer_value(json_array_get(params, 0));

    market_t *market = get_market(settings.markets[0].name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    order_t *order = market_get_order(market, order_id);
    if (order == NULL) {
        return reply_error(ses, pkg, 13, "envelope not found");
    }

    json_t *result = NULL;
    int ret = market_cancel_order(true, &result, market, order);
    if (ret < 0) {
        log_fatal("cancel order: %"PRIu64" fail: %d", order->id, ret);
        return reply_error_internal_error(ses, pkg);
    }

    append_operlog("cancel_order", params);
    ret = reply_result(ses, pkg, result, true);
    json_decref(result);
    return ret;
}

static void svr_on_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    json_t *params = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (params == NULL || !json_is_array(params)) {
        goto decode_error;
    }
    sds params_str = sdsnewlen(pkg->body, pkg->body_size);

    int ret;
    switch (pkg->command) {
    case CMD_ASSET_LIST:
        log_trace("from: %s cmd asset list, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_asset_list(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_asset_list %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ASSET_UPDATE:
        log_trace("from: %s cmd asset update, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_asset_update(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_asset_update %s fail: %d", params_str, ret);
        }
        break;

    // envelope
    case CMD_ENVELOPE_PUT:
        if (is_operlog_block() || is_history_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d", is_operlog_block(), is_history_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd envelope put, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_envelope_put(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_envelope_put %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ENVELOPE_OPEN:
        if (is_operlog_block() || is_history_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d", is_operlog_block(), is_history_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd envelope open, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_envelope_open(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_envelope_open %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ENVELOPE_HISTORY:
        if (is_operlog_block() || is_history_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d", is_operlog_block(), is_history_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd envelope history, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_envelope_history(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_envelope_history %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ENVELOPE_DETAIL:
        if (is_operlog_block() || is_history_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d", is_operlog_block(), is_history_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd envelope detail, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_envelope_detail(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_envelope_detail %s fail: %d", params_str, ret);
        }
        break;

    case CMD_ORDER_QUERY:
        log_trace("from: %s cmd order query, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_query(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_query %s fail: %d", params_str, ret);
        }
        break;

    case CMD_ORDER_BOOK:
        log_trace("from: %s cmd order book, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_book(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_book %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_CANCEL:
        if (is_operlog_block() || is_history_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d", is_operlog_block(), is_history_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd order cancel, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_cancel(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_cancel %s fail: %d", params_str, ret);
        }
        break;

    default:
        log_error("from: %s unknown command: %u", nw_sock_human_addr(&ses->peer_addr), pkg->command);
        break;
    }

cleanup:
    sdsfree(params_str);
    json_decref(params);
    return;

decode_error:
    if (params) {
        json_decref(params);
    }
    sds hex = hexdump(pkg->body, pkg->body_size);
    log_error("connection: %s, cmd: %u decode params fail, params data: \n%s", \
            nw_sock_human_addr(&ses->peer_addr), pkg->command, hex);
    sdsfree(hex);
    rpc_svr_close_clt(svr, ses);

    return;
}

static void svr_on_new_connection(nw_ses *ses)
{
    log_trace("new connection: %s", nw_sock_human_addr(&ses->peer_addr));
}

static void svr_on_connection_close(nw_ses *ses)
{
    log_trace("connection: %s close", nw_sock_human_addr(&ses->peer_addr));
}

static uint32_t cache_dict_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sdslen((sds)key));
}

static int cache_dict_key_compare(const void *key1, const void *key2)
{
    return sdscmp((sds)key1, (sds)key2);
}

static void *cache_dict_key_dup(const void *key)
{
    return sdsdup((const sds)key);
}

static void cache_dict_key_free(void *key)
{
    sdsfree(key);
}

static void *cache_dict_val_dup(const void *val)
{
    struct cache_val *obj = malloc(sizeof(struct cache_val));
    memcpy(obj, val, sizeof(struct cache_val));
    return obj;
}

static void cache_dict_val_free(void *val)
{
    struct cache_val *obj = val;
    json_decref(obj->result);
    free(val);
}

static void on_cache_timer(nw_timer *timer, void *privdata)
{
    dict_clear(dict_cache);
}

int init_server(void)
{
    rpc_svr_type type;
    memset(&type, 0, sizeof(type));
    type.on_recv_pkg = svr_on_recv_pkg;
    type.on_new_connection = svr_on_new_connection;
    type.on_connection_close = svr_on_connection_close;

    svr = rpc_svr_create(&settings.svr, &type);
    if (svr == NULL)
        return -__LINE__;
    if (rpc_svr_start(svr) < 0)
        return -__LINE__;

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = cache_dict_hash_function;
    dt.key_compare    = cache_dict_key_compare;
    dt.key_dup        = cache_dict_key_dup;
    dt.key_destructor = cache_dict_key_free;
    dt.val_dup        = cache_dict_val_dup;
    dt.val_destructor = cache_dict_val_free;

    dict_cache = dict_create(&dt, 64);
    if (dict_cache == NULL)
        return -__LINE__;

    nw_timer_set(&cache_timer, 60, true, on_cache_timer, NULL);
    nw_timer_start(&cache_timer);

    return 0;
}


