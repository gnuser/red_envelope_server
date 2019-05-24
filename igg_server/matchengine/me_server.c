/*
 * Description:
 *     History: yang@haipo.me, 2017/03/16, create
 */
#include <curl/curl.h>
# include "me_config.h"
# include "me_server.h"
# include "me_balance.h"
# include "me_update.h"
# include "me_market.h"
# include "me_trade.h"
# include "me_operlog.h"
# include "me_history.h"
# include "me_message.h"


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

static int on_cmd_balance_query(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    size_t request_size = json_array_size(params);
    if (request_size == 0)
        return reply_error_invalid_argument(ses, pkg);

    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));
    if (user_id == 0)
        return reply_error_invalid_argument(ses, pkg);

    json_t *result = json_object();
    if (request_size == 1) {
        for (size_t i = 0; i < settings.asset_num; ++i) {
            const char *asset = settings.assets[i].name;
            json_t *unit = json_object();
            int prec_save = asset_prec(asset);
            int prec_show = asset_prec_show(asset);

            mpd_t *available = balance_get(user_id, BALANCE_TYPE_AVAILABLE, asset);
            if (available) {
                if (prec_save != prec_show) {
                    mpd_t *show = mpd_qncopy(available);
                    mpd_rescale(show, show, -prec_show, &mpd_ctx);
                    json_object_set_new_mpd(unit, "available", show);
                    mpd_del(show);
                } else {
                    json_object_set_new_mpd(unit, "available", available);
                }
            } else {
                json_object_set_new(unit, "available", json_string("0"));
            }

            mpd_t *freeze = balance_get(user_id, BALANCE_TYPE_FREEZE, asset);
            if (freeze) {
                if (prec_save != prec_show) {
                    mpd_t *show = mpd_qncopy(freeze);
                    mpd_rescale(show, show, -prec_show, &mpd_ctx);
                    json_object_set_new_mpd(unit, "freeze", show);
                    mpd_del(show);
                } else {
                    json_object_set_new_mpd(unit, "freeze", freeze);
                }
            } else {
                json_object_set_new(unit, "freeze", json_string("0"));
            }
#ifdef FREEZE_BALANCE
            mpd_t* pledge = balance_get(user_id,BALANCE_TYPE_PLEDGE,asset);
            if (pledge) {
                if (prec_save != prec_show) {
                    mpd_t *show = mpd_qncopy(pledge);
                    mpd_rescale(show, show, -prec_show, &mpd_ctx);
                    json_object_set_new_mpd(unit, "pledge", show);
                    mpd_del(show);
                } else {
                    json_object_set_new_mpd(unit, "pledge", pledge);
                }
            } else {
                json_object_set_new(unit, "pledge", json_string("0"));
            }

            mpd_t* settle = balance_get(user_id,BALANCE_TYPE_SETTLE,asset);
            if (settle) {
                if (prec_save != prec_show) {
                    mpd_t *show = mpd_qncopy(settle);
                    mpd_rescale(show, show, -prec_show, &mpd_ctx);
                    json_object_set_new_mpd(unit, "settle", show);
                    mpd_del(show);
                } else {
                    json_object_set_new_mpd(unit, "settle", settle);
                }
            } else {
                json_object_set_new(unit, "settle", json_string("0"));
            }


            mpd_t* negative = balance_get(user_id,BALANCE_TYPE_NEGATIVE,asset);
            if (negative) {
                if (prec_save != prec_show) {
                    mpd_t *show = mpd_qncopy(negative);
                    mpd_rescale(show, show, -prec_show, &mpd_ctx);
                    json_object_set_new_mpd(unit, "nagative", show);
                    mpd_del(show);
                } else {
                    json_object_set_new_mpd(unit, "negative", negative);
                }
            } else {
                json_object_set_new(unit, "negative", json_string("0"));
            }


            mpd_t* rewards = balance_get(user_id,BALANCE_TYPE_REWARDS,asset);
            if (rewards) {
                if (prec_save != prec_show) {
                    mpd_t *show = mpd_qncopy(negative);
                    mpd_rescale(show, show, -prec_show, &mpd_ctx);
                    json_object_set_new_mpd(unit, "rewards", show);
                    mpd_del(show);
                } else {
                    json_object_set_new_mpd(unit, "rewards", rewards);
                }
            } else {
                json_object_set_new(unit, "rewards", json_string("0"));
            }


            mpd_t* airdrops = balance_get(user_id,BALANCE_TYPE_AIRDROP,asset);
            if (airdrops) {
                if (prec_save != prec_show) {
                    mpd_t *show = mpd_qncopy(airdrops);
                    mpd_rescale(show, show, -prec_show, &mpd_ctx);
                    json_object_set_new_mpd(unit, "airdrops", show);
                    mpd_del(show);
                } else {
                    json_object_set_new_mpd(unit, "airdrop", airdrops);
                }
            } else {
                json_object_set_new(unit, "airdrop", json_string("0"));
            }



#endif
            json_object_set_new(result, asset, unit);
        }
    } else {
        for (size_t i = 1; i < request_size; ++i) {
            const char *asset = json_string_value(json_array_get(params, i));
            if (!asset || !asset_exist(asset)) {
                json_decref(result);
                return reply_error_invalid_argument(ses, pkg);
            }
            json_t *unit = json_object();
            int prec_save = asset_prec(asset);
            int prec_show = asset_prec_show(asset);

            mpd_t *available = balance_get(user_id, BALANCE_TYPE_AVAILABLE, asset);
            if (available) {
                if (prec_save != prec_show) {
                    mpd_t *show = mpd_qncopy(available);
                    mpd_rescale(show, show, -prec_show, &mpd_ctx);
                    json_object_set_new_mpd(unit, "available", show);
                    mpd_del(show);
                } else {
                    json_object_set_new_mpd(unit, "available", available);
                }
            } else {
                json_object_set_new(unit, "available", json_string("0"));
            }

            mpd_t *freeze = balance_get(user_id, BALANCE_TYPE_FREEZE, asset);
            if (freeze) {
                if (prec_save != prec_show) {
                    mpd_t *show = mpd_qncopy(freeze);
                    mpd_rescale(show, show, -prec_show, &mpd_ctx);
                    json_object_set_new_mpd(unit, "freeze", show);
                    mpd_del(show);
                } else {
                    json_object_set_new_mpd(unit, "freeze", freeze);
                }
            } else {
                json_object_set_new(unit, "freeze", json_string("0"));
            }
#ifdef FREEZE_BALANCE
            mpd_t* pledge = balance_get(user_id,BALANCE_TYPE_PLEDGE,asset);
            if (pledge) {
                if (prec_save != prec_show) {
                    mpd_t *show = mpd_qncopy(pledge);
                    mpd_rescale(show, show, -prec_show, &mpd_ctx);
                    json_object_set_new_mpd(unit, "pledge", show);
                    mpd_del(show);
                } else {
                    json_object_set_new_mpd(unit, "pledge", pledge);
                }
            } else {
                json_object_set_new(unit, "pledge", json_string("0"));
            }

            mpd_t* settle = balance_get(user_id,BALANCE_TYPE_SETTLE,asset);
            if (settle) {
                if (prec_save != prec_show) {
                    mpd_t *show = mpd_qncopy(settle);
                    mpd_rescale(show, show, -prec_show, &mpd_ctx);
                    json_object_set_new_mpd(unit, "settle", show);
                    mpd_del(show);
                } else {
                    json_object_set_new_mpd(unit, "settle", settle);
                }
            } else {
                json_object_set_new(unit, "settle", json_string("0"));
            }

            mpd_t* negative = balance_get(user_id,BALANCE_TYPE_NEGATIVE,asset);
            if (negative) {
                if (prec_save != prec_show) {
                    mpd_t *show = mpd_qncopy(negative);
                    mpd_rescale(show, show, -prec_show, &mpd_ctx);
                    json_object_set_new_mpd(unit, "negative", show);
                    mpd_del(show);
                } else {
                    json_object_set_new_mpd(unit, "negative", negative);
                }
            } else {
                json_object_set_new(unit, "negative", json_string("0"));
            }

            mpd_t* rewards = balance_get(user_id,BALANCE_TYPE_REWARDS,asset);
            if (rewards) {
                if (prec_save != prec_show) {
                    mpd_t *show = mpd_qncopy(rewards);
                    mpd_rescale(show, show, -prec_show, &mpd_ctx);
                    json_object_set_new_mpd(unit, "rewards", show);
                    mpd_del(show);
                } else {
                    json_object_set_new_mpd(unit, "rewards", rewards);
                }
            } else {
                json_object_set_new(unit, "rewards", json_string("0"));
            }

            mpd_t* airdrops = balance_get(user_id,BALANCE_TYPE_AIRDROP,asset);
            if (airdrops) {
                if (prec_save != prec_show) {
                    mpd_t *show = mpd_qncopy(airdrops);
                    mpd_rescale(show, show, -prec_show, &mpd_ctx);
                    json_object_set_new_mpd(unit, "airdrop", show);
                    mpd_del(show);
                } else {
                    json_object_set_new_mpd(unit, "airdrop", airdrops);
                }
            } else {
                json_object_set_new(unit, "airdrop", json_string("0"));
            }
#endif
            json_object_set_new(result, asset, unit);
        }
    }

    int ret = reply_result(ses, pkg, result, true);
    json_decref(result);
    return ret;
}

static int on_cmd_balance_update(nw_ses *ses, rpc_pkg *pkg, json_t *params)
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

    // business
    if (!json_is_string(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    const char *business = json_string_value(json_array_get(params, 2));

    // business_id
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t business_id = json_integer_value(json_array_get(params, 3));

    // change
    if (!json_is_string(json_array_get(params, 4)))
        return reply_error_invalid_argument(ses, pkg);
    mpd_t *change = decimal(json_string_value(json_array_get(params, 4)), prec);
    if (change == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // detail
    json_t *detail = json_array_get(params, 5);
    if (!json_is_object(detail)) {
        mpd_del(change);
        return reply_error_invalid_argument(ses, pkg);
    }

    int ret = update_user_balance(true, user_id, asset, business, business_id, change, detail);
    mpd_del(change);
    if (ret == -1) {
        return reply_error(ses, pkg, 10, "repeat update");
    } else if (ret == -2) {
        return reply_error(ses, pkg, 11, "balance not enough");
    } else if (ret < 0) {
        return reply_error_internal_error(ses, pkg);
    }

    append_operlog("update_balance", params);
    return reply_success(ses, pkg);
}

#ifdef FREEZE_BALANCE
static int on_cmd_balance_freeze(nw_ses *ses, rpc_pkg *pkg, json_t *params)
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

    // business
    if (!json_is_string(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    const char *business = json_string_value(json_array_get(params, 2));

    // business_id
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t business_id = json_integer_value(json_array_get(params, 3));

    // change
    if (!json_is_string(json_array_get(params, 4)))
        return reply_error_invalid_argument(ses, pkg);
    mpd_t *change = decimal(json_string_value(json_array_get(params, 4)), prec);
    if (change == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // detail
    json_t *detail = json_array_get(params, 5);
    if (!json_is_object(detail)) {
        mpd_del(change);
        return reply_error_invalid_argument(ses, pkg);
    }

    int ret = pledge_user_balance(true, user_id, asset, business, business_id, change, detail);
    mpd_del(change);
    if (ret == -1) {
        return reply_error(ses, pkg, 10, "repeat update");
    } else if (ret == -2) {
        return reply_error(ses, pkg, 11, "balance not enough");
    } else if (ret < 0) {
        return reply_error_internal_error(ses, pkg);
    }

    append_operlog("freeze_balance", params);
    return reply_success(ses, pkg);
}


static int on_cmd_balance_withdraw(nw_ses *ses, rpc_pkg *pkg, json_t *params)
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

    // business
    if (!json_is_string(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    const char *business = json_string_value(json_array_get(params, 2));

    // business_id
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t business_id = json_integer_value(json_array_get(params, 3));

    // change
    if (!json_is_string(json_array_get(params, 4)))
        return reply_error_invalid_argument(ses, pkg);
    mpd_t *change = decimal(json_string_value(json_array_get(params, 4)), prec);
    if (change == NULL)
        return reply_error_invalid_argument(ses, pkg);
    
    // detail
    json_t *detail = json_array_get(params, 5);
    if (!json_is_object(detail)) {
        mpd_del(change);
        return reply_error_invalid_argument(ses, pkg);
    }
    int ret = update_user_pledge(true, user_id, asset, business, business_id, change, detail);
    if (ret == -1) {
        mpd_del(change);
        return reply_error(ses, pkg, 10, "repeat update");
    } else if (ret == -2) {
        mpd_del(change);
        return reply_error(ses, pkg, 11, "balance not enough");
    } else if (ret < 0) {
        mpd_del(change);
        return reply_error_internal_error(ses, pkg);
    }

    if (mpd_cmp(change, mpd_zero, &mpd_ctx) > 0) {
        append_operlog("update_balance", params);
        append_operlog("freeze_balance", params);
    } else {
        append_operlog("freeze_balance", params);
        append_operlog("update_balance", params);
    }
    mpd_del(change);

    return reply_success(ses, pkg);
}

static int on_cmd_balance_settle(nw_ses *ses, rpc_pkg *pkg, json_t *params)
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

    // business
    if (!json_is_string(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    const char *business = json_string_value(json_array_get(params, 2));

    // business_id
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t business_id = json_integer_value(json_array_get(params, 3));

    // change
    if (!json_is_string(json_array_get(params, 4)))
        return reply_error_invalid_argument(ses, pkg);
    mpd_t *change = decimal(json_string_value(json_array_get(params, 4)), prec);
    if (change == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // detail
    json_t *detail = json_array_get(params, 5);
    if (!json_is_object(detail)) {
        mpd_del(change);
        return reply_error_invalid_argument(ses, pkg);
    }

    int ret = settle_user_balance(true, user_id, asset, business, business_id, change, detail);
    mpd_del(change);
    if (ret == -1) {
        return reply_error(ses, pkg, 10, "repeat update");
    } else if (ret == -2) {
        return reply_error(ses, pkg, 11, "balance not enough");
    } else if (ret < 0) {
        return reply_error_internal_error(ses, pkg);
    }

    append_operlog("settle_balance", params);
    return reply_success(ses, pkg);
}

static int on_cmd_balance_release(nw_ses *ses, rpc_pkg *pkg, json_t *params)
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

    // business
    if (!json_is_string(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    const char *business = json_string_value(json_array_get(params, 2));

    // business_id
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t business_id = json_integer_value(json_array_get(params, 3));

    // change
    if (!json_is_string(json_array_get(params, 4)))
        return reply_error_invalid_argument(ses, pkg);
    mpd_t *change = decimal(json_string_value(json_array_get(params, 4)), prec);
    if (change == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // detail
    json_t *detail = json_array_get(params, 5);
    if (!json_is_object(detail)) {
        mpd_del(change);
        return reply_error_invalid_argument(ses, pkg);
    }

    int ret = release_user_balance(true, user_id, asset, business, business_id, change, detail);
    mpd_del(change);
    if (ret == -1) {
        return reply_error(ses, pkg, 10, "repeat update");
    } else if (ret == -2) {
        return reply_error(ses, pkg, 11, "balance not enough");
    } else if (ret < 0) {
        return reply_error_internal_error(ses, pkg);
    }

    append_operlog("release_balance", params);
    return reply_success(ses, pkg);
}

static int on_cmd_balance_exception(nw_ses *ses, rpc_pkg *pkg, json_t *params)
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

    // business
    if (!json_is_string(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    const char *business = json_string_value(json_array_get(params, 2));

    // business_id
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t business_id = json_integer_value(json_array_get(params, 3));

    // change
    if (!json_is_string(json_array_get(params, 4)))
        return reply_error_invalid_argument(ses, pkg);
    mpd_t *change = decimal(json_string_value(json_array_get(params, 4)), prec);
    if (change == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // detail
    json_t *detail = json_array_get(params, 5);
    if (!json_is_object(detail)) {
        mpd_del(change);
        return reply_error_invalid_argument(ses, pkg);
    }

    int ret = exception_user_balance(true, user_id, asset, business, business_id, change, detail);
    mpd_del(change);
    if (ret == -1) {
        return reply_error(ses, pkg, 10, "repeat update");
    } else if (ret == -2) {
        return reply_error(ses, pkg, 11, "balance not enough");
    } else if (ret < 0) {
        return reply_error_internal_error(ses, pkg);
    }

    append_operlog("execption_balance", params);
    return reply_success(ses, pkg);
}

static int on_cmd_balance_reward(nw_ses *ses, rpc_pkg *pkg, json_t *params)
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

    // business
    if (!json_is_string(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    const char *business = json_string_value(json_array_get(params, 2));

    // business_id
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t business_id = json_integer_value(json_array_get(params, 3));

    // change
    if (!json_is_string(json_array_get(params, 4)))
        return reply_error_invalid_argument(ses, pkg);
    mpd_t *change = decimal(json_string_value(json_array_get(params, 4)), prec);
    if (change == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // detail
    json_t *detail = json_array_get(params, 5);
    if (!json_is_object(detail)) {
        mpd_del(change);
        return reply_error_invalid_argument(ses, pkg);
    }

    int ret = reward_user_balance(true, user_id, asset, business, business_id, change, detail);
    mpd_del(change);
    if (ret == -1) {
        return reply_error(ses, pkg, 10, "repeat update");
    } else if (ret == -2) {
        return reply_error(ses, pkg, 11, "balance not enough");
    } else if (ret < 0) {
        return reply_error_internal_error(ses, pkg);
    }

    append_operlog("reward_balance", params);
    return reply_success(ses, pkg);
}

static int on_cmd_balance_airdrop(nw_ses *ses, rpc_pkg *pkg, json_t *params)
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

    // business
    if (!json_is_string(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    const char *business = json_string_value(json_array_get(params, 2));

    // business_id
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t business_id = json_integer_value(json_array_get(params, 3));

    // change
    if (!json_is_string(json_array_get(params, 4)))
        return reply_error_invalid_argument(ses, pkg);
    mpd_t *change = decimal(json_string_value(json_array_get(params, 4)), prec);
    if (change == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // detail
    json_t *detail = json_array_get(params, 5);
    if (!json_is_object(detail)) {
        mpd_del(change);
        return reply_error_invalid_argument(ses, pkg);
    }

    int ret = airdrop_user_balance(true, user_id, asset, business, business_id, change, detail);
    mpd_del(change);
    if (ret == -1) {
        return reply_error(ses, pkg, 10, "repeat update");
    } else if (ret == -2) {
        return reply_error(ses, pkg, 11, "balance not enough");
    } else if (ret < 0) {
        return reply_error_internal_error(ses, pkg);
    }

    append_operlog("airdrop_balance", params);
    return reply_success(ses, pkg);
}

static int on_cmd_balance_addnegactive(nw_ses *ses, rpc_pkg *pkg, json_t *params)
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

    // business
    if (!json_is_string(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    const char *business = json_string_value(json_array_get(params, 2));

    // business_id
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t business_id = json_integer_value(json_array_get(params, 3));

    // change
    if (!json_is_string(json_array_get(params, 4)))
        return reply_error_invalid_argument(ses, pkg);
    mpd_t *change = decimal(json_string_value(json_array_get(params, 4)), prec);
    if (change == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // detail
    json_t *detail = json_array_get(params, 5);
    if (!json_is_object(detail)) {
        mpd_del(change);
        return reply_error_invalid_argument(ses, pkg);
    }

    int ret = addnegactive_user_balance(true, user_id, asset, business, business_id, change, detail);
    mpd_del(change);
    if (ret == -1) {
        return reply_error(ses, pkg, 10, "repeat update");
    } else if (ret == -2) {
        return reply_error(ses, pkg, 11, "balance not enough");
    } else if (ret < 0) {
        return reply_error_internal_error(ses, pkg);
    }

    append_operlog("addnegactive_balance", params);
    return reply_success(ses, pkg);
}
#endif

//#ifdef CONVERSION


static int on_cmd_order_put_order(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    //put_conversion(role,user_id,stock,money,volume,price) --> maker调用形式
    //put_conversion(role,user_id,stock,money,volume,conversion_id) --->toker调用形式
    if (json_array_size(params) != 6 )
        return reply_error_invalid_argument(ses, pkg);

    // role
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t role = json_integer_value(json_array_get(params, 0));

    // user_id
    if (!json_is_integer(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 1));

    //stock name
    if (!json_is_string(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    const char *stock_name = json_string_value(json_array_get(params, 2));

    //money name
    if (!json_is_string(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    const char *money_name = json_string_value(json_array_get(params, 3));

    //check market
    sds market_name = sdsempty();
    sdscpy(market_name,stock_name);
    market_name = sdscat(market_name,money_name);
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    //quantity
    if (!json_is_string(json_array_get(params, 4)))
        return reply_error_invalid_argument(ses, pkg);
    const char *quantity = json_string_value(json_array_get(params, 4));
    //price
    if (!json_is_string(json_array_get(params, 5)))
        return reply_error_invalid_argument(ses, pkg);


    if(1 == role)
    {
        mpd_t* amount = decimal(quantity, market->stock_prec);
        mpd_t* price     = decimal(json_string_value(json_array_get(params, 5)), market->money_prec);
        json_t *ret_result = NULL;
        int ret =  market_add_order(true,&ret_result,market,user_id,amount,price,true);
        mpd_del(amount);
        mpd_del(price);

        if (ret < 0)
        {
            log_fatal("on_cmd_order_put_order fail: %d", ret);
            return reply_error_internal_error(ses, pkg);
        }

        append_operlog("put_order", params);
        ret = reply_result(ses, pkg, ret_result, true);
        json_decref(ret_result);
        return ret;
    }
    else if(2 == role)
    {
        // conversion_id
        mpd_t* amount    = decimal(quantity, market->stock_prec);
        mpd_t* price     = decimal(json_string_value(json_array_get(params, 5)), market->money_prec);

        json_t *ret_result = NULL;
        int ret =  market_add_order(true,&ret_result,market,user_id,amount,price,false);
        if (ret < 0)
        {
            log_fatal("on_cmd_order_put_order fail: %d", ret);
            return reply_error_internal_error(ses, pkg);
        }
        mpd_del(amount);
        mpd_del(price);
        append_operlog("put_order", params);
        ret = reply_result(ses, pkg, ret_result, true);
        json_decref(ret_result);
        return ret;
    }


    return reply_error_invalid_argument(ses, pkg);
}

static int on_cmd_order_put_match(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 6 )
        return reply_error_invalid_argument(ses, pkg);

    // ask_uid
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t ask_uid = json_integer_value(json_array_get(params, 0));

    // bid_uid
    if (!json_is_integer(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t bid_uid = json_integer_value(json_array_get(params, 1));

    //stock name
    if (!json_is_string(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    const char *stock_name = json_string_value(json_array_get(params, 2));

    //money name
    if (!json_is_string(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    const char *money_name = json_string_value(json_array_get(params, 3));


    //check market
    sds market_name = sdsempty();
    sdscpy(market_name,stock_name);
    market_name = sdscat(market_name,money_name);
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // ask_orderid
    if (!json_is_integer(json_array_get(params, 4)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t ask_orderid = json_integer_value(json_array_get(params, 4));

    // bid_orderid
    if (!json_is_integer(json_array_get(params, 5)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t bid_orderid = json_integer_value(json_array_get(params, 5));

    json_t *ret_result = NULL;

    int ret = order_put_match(true,&ret_result,market,ask_orderid,bid_orderid);
    if (ret == -100)
    {
        return reply_error(ses, pkg, 100, "order id not find or has been matched!");
    }
    else if(ret == -101)
    {
        return reply_error(ses, pkg, 101, "order side error");
    }
    else if(ret == -102)
    {
        return reply_error(ses, pkg, 102, "orders price is not same");
    }
    else if(ret == -103)
    {
        return reply_error(ses,pkg, 103, "ask pledge  not enough");
    }
    else if(ret == -104)
    {
        return reply_error(ses,pkg, 104,"bid pledge  not enough" );
    }


    if (ret !=0 )
    {
        return reply_error_invalid_argument(ses, pkg);
    }

    append_operlog("put_match", params);
    ret = reply_result(ses, pkg, ret_result, true);
    json_decref(ret_result);
    return ret;
}


//#endif

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

static int on_cmd_market_update(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 3)
        return reply_error_invalid_argument(ses, pkg);

    if (!json_is_string(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);

    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);

    if (!json_is_string(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);

    int ret = init_asset_and_market(true);
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "market update fail: %d", ret);
    }

    return reply_success(ses, pkg);
}   

static int on_cmd_asset_update(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 1)
        return reply_error_invalid_argument(ses, pkg);

    if (!json_is_string(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);

    const char *asset = json_string_value(json_array_get(params, 0));
    int ret = asset_update(asset);
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "asset update1 fail: %d", ret);
    }

    ret = init_asset_and_market(false);
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "asset update2 fail: %d", ret);
    }
    
    init_balance();
    return reply_success(ses, pkg);
}   

static json_t *get_asset_summary(const char *name)
{
    size_t available_count;
    size_t freeze_count;
    mpd_t *total = mpd_new(&mpd_ctx);
    mpd_t *available = mpd_new(&mpd_ctx);
    mpd_t *freeze = mpd_new(&mpd_ctx);
    // balance_status(name, total, &available_count, available, &freeze_count, freeze);
    size_t pledge_count;
    mpd_t *pledge = mpd_new(&mpd_ctx);
    balance_status(name, total, &available_count, available, &freeze_count, freeze, &pledge_count, pledge);

    json_t *obj = json_object();
    json_object_set_new(obj, "name", json_string(name));
    json_object_set_new_mpd(obj, "total_balance", total);
    json_object_set_new(obj, "available_count", json_integer(available_count));
    json_object_set_new_mpd(obj, "available_balance", available);
    json_object_set_new(obj, "freeze_count", json_integer(freeze_count));
    json_object_set_new_mpd(obj, "freeze_balance", freeze);
    json_object_set_new(obj, "pledge_count", json_integer(pledge_count));
    json_object_set_new_mpd(obj, "pledge_balance", pledge);

    mpd_del(total);
    mpd_del(available);
    mpd_del(freeze);
    mpd_del(pledge);

    return obj;
}

static int on_cmd_asset_summary(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    json_t *result = json_array();
    if (json_array_size(params) == 0) {
        for (int i = 0; i < settings.asset_num; ++i) {
            json_array_append_new(result, get_asset_summary(settings.assets[i].name));
        }
    } else {
        for (int i = 0; i < json_array_size(params); ++i) {
            const char *asset = json_string_value(json_array_get(params, i));
            if (asset == NULL)
                goto invalid_argument;
            if (!asset_exist(asset))
                goto invalid_argument;
            json_array_append_new(result, get_asset_summary(asset));
        }
    }

    int ret = reply_result(ses, pkg, result, false);
    json_decref(result);
    return ret;

invalid_argument:
    json_decref(result);
    return reply_error_invalid_argument(ses, pkg);
}


// token discount
// order.put_limit (uid, market, side, amount, price, taker_fee_rate, maker_fee_rate, source, token, discount)
static int on_cmd_order_put_limit(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 10)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // side
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 2));
    if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return reply_error_invalid_argument(ses, pkg);

    mpd_t *amount    = NULL;
    mpd_t *price     = NULL;
    mpd_t *taker_fee = NULL;
    mpd_t *maker_fee = NULL;
    mpd_t *discount = mpd_new(&mpd_ctx);
    mpd_t *token_rate = mpd_new(&mpd_ctx);
    mpd_t *asset_rate = mpd_new(&mpd_ctx);

    // amount
    if (!json_is_string(json_array_get(params, 3)))
        goto invalid_argument;
    amount = decimal(json_string_value(json_array_get(params, 3)), market->stock_prec);
    if (amount == NULL || mpd_cmp(amount, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // price
    if (!json_is_string(json_array_get(params, 4)))
        goto invalid_argument;
    price = decimal(json_string_value(json_array_get(params, 4)), market->money_prec);
    if (price == NULL || mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // taker fee
    if (!json_is_string(json_array_get(params, 5)))
        goto invalid_argument;
    taker_fee = decimal(json_string_value(json_array_get(params, 5)), market->fee_prec);
    if (taker_fee == NULL || mpd_cmp(taker_fee, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(taker_fee, mpd_one, &mpd_ctx) >= 0)
        goto invalid_argument;

    // maker fee
    if (!json_is_string(json_array_get(params, 6)))
        goto invalid_argument;
    maker_fee = decimal(json_string_value(json_array_get(params, 6)), market->fee_prec);
    if (maker_fee == NULL || mpd_cmp(maker_fee, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(maker_fee, mpd_one, &mpd_ctx) >= 0)
        goto invalid_argument;

    // source
    if (!json_is_string(json_array_get(params, 7)))
        goto invalid_argument;
    const char *source = json_string_value(json_array_get(params, 7));
    if (strlen(source) >= SOURCE_MAX_LEN)
        goto invalid_argument;

    // token
    if (!json_is_string(json_array_get(params, 8)))
        return reply_error_invalid_argument(ses, pkg);
    const char *token = json_string_value(json_array_get(params, 8));

    // discount
    if (strlen(token) == 0) {
        mpd_copy(discount, mpd_zero, &mpd_ctx);
        mpd_copy(token_rate, mpd_zero, &mpd_ctx);
        mpd_copy(asset_rate, mpd_zero, &mpd_ctx);
    } else {
        if (asset_exist(token)) {
            if (!json_is_string(json_array_get(params, 9)))
                goto invalid_argument;
            discount = decimal(json_string_value(json_array_get(params, 9)), 0);
            if (discount == NULL || mpd_cmp(discount, mpd_zero, &mpd_ctx) <= 0)
                goto invalid_argument;
        } else {
            mpd_del(discount);
            return reply_error(ses, pkg, 16, "token is not exist");
        }

        const char *cny = "CNY";
        sds token_cny = sdsempty();
        sdscpy(token_cny, token);
        token_cny = sdscat(token_cny, cny);

        if (get_market(token_cny) == NULL)
            goto invalid_argument;

        char *asset = (side == 1) ? strdup(market->money) : strdup(market->stock);
        if (strcmp(asset, cny) == 0) {
            for (size_t i = 0; i < settings.market_num; ++i) {
                if (strcmp(token_cny, settings.markets[i].name) == 0) {
                    mpd_copy(token_rate, settings.markets[i].last, &mpd_ctx);
                }
            }
            mpd_copy(asset_rate, mpd_one, &mpd_ctx);
        } else {
            sds asset_cny = sdsempty();
            sdscpy(asset_cny, asset);
            asset_cny = sdscat(asset_cny, cny);
            if (get_market(asset_cny) == NULL)
                goto invalid_argument;

            for (size_t i = 0; i < settings.market_num; ++i) {
                if (strcmp(token_cny, settings.markets[i].name) == 0) {
                    mpd_copy(token_rate, settings.markets[i].last, &mpd_ctx);
                }
                if (strcmp(asset_cny, settings.markets[i].name) == 0) {
                    mpd_copy(asset_rate, settings.markets[i].last, &mpd_ctx);
                }
            }
        }
    }

    json_t *result = NULL;
    int ret = market_put_limit_order(true, &result, market, user_id, side, amount, price, taker_fee, maker_fee,
                                     source, token, discount, token_rate, asset_rate);

    mpd_del(amount);
    mpd_del(price);
    mpd_del(taker_fee);
    mpd_del(maker_fee);
    mpd_del(discount);
    mpd_del(token_rate);
    mpd_del(asset_rate);

    if (ret == -1) {
        return reply_error(ses, pkg, 11, "balance not enough");
    } else if (ret == -2) {
        return reply_error(ses, pkg, 12, "amount too small");
    } else if (ret == -4) {
        return reply_error(ses, pkg, 17, "rate is zero");
    } else if (ret < 0) {
        log_fatal("market_put_limit_order fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }

    append_operlog("limit_order", params);
    ret = reply_result(ses, pkg, result, true);
    json_decref(result);
    return ret;

invalid_argument:
    if (amount)
        mpd_del(amount);
    if (price)
        mpd_del(price);
    if (taker_fee)
        mpd_del(taker_fee);
    if (maker_fee)
        mpd_del(maker_fee);
    if (discount)
        mpd_del(discount);
    if (token_rate)
        mpd_del(token_rate);
    if (asset_rate)
        mpd_del(asset_rate);

    return reply_error_invalid_argument(ses, pkg);
}


// token discount
// order.put_market (uid, market, side, amount, taker_fee_rate, source, token, discount)
static int on_cmd_order_put_market(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 8)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // side
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 2));
    if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return reply_error_invalid_argument(ses, pkg);

    mpd_t *amount = NULL;
    mpd_t *taker_fee = NULL;
    mpd_t *discount = mpd_new(&mpd_ctx);
    mpd_t *token_rate = mpd_new(&mpd_ctx);
    mpd_t *asset_rate = mpd_new(&mpd_ctx);

    // amount
    if (!json_is_string(json_array_get(params, 3)))
        goto invalid_argument;
    amount = decimal(json_string_value(json_array_get(params, 3)), market->stock_prec);
    if (amount == NULL || mpd_cmp(amount, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // taker fee
    if (!json_is_string(json_array_get(params, 4)))
        goto invalid_argument;
    taker_fee = decimal(json_string_value(json_array_get(params, 4)), market->fee_prec);
    if (taker_fee == NULL || mpd_cmp(taker_fee, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(taker_fee, mpd_one, &mpd_ctx) >= 0)
        goto invalid_argument;

    // source
    if (!json_is_string(json_array_get(params, 5)))
        goto invalid_argument;
    const char *source = json_string_value(json_array_get(params, 5));
    if (strlen(source) >= SOURCE_MAX_LEN)
        goto invalid_argument;

    // token
    if (!json_is_string(json_array_get(params, 6)))
        return reply_error_invalid_argument(ses, pkg);
    const char *token = json_string_value(json_array_get(params, 6));

    // discount
    if (strlen(token) == 0) {
        mpd_copy(discount, mpd_zero, &mpd_ctx);
        mpd_copy(token_rate, mpd_zero, &mpd_ctx);
        mpd_copy(asset_rate, mpd_zero, &mpd_ctx);
    } else {
        if (asset_exist(token)) {
            if (!json_is_string(json_array_get(params, 7)))
                goto invalid_argument;
            discount = decimal(json_string_value(json_array_get(params, 7)), 0);
            if (discount == NULL || mpd_cmp(discount, mpd_zero, &mpd_ctx) <= 0)
                goto invalid_argument;
        } else {
            mpd_del(discount);
            return reply_error(ses, pkg, 16, "token is not exist");
        }

        const char *cny = "CNY";
        sds token_cny = sdsempty();
        sdscpy(token_cny, token);
        token_cny = sdscat(token_cny, cny);

        if (get_market(token_cny) == NULL)
            goto invalid_argument;

        char *asset = (side == 1) ? strdup(market->money) : strdup(market->stock);
        if (strcmp(asset, cny) == 0) {
            for (size_t i = 0; i < settings.market_num; ++i) {
                if (strcmp(token_cny, settings.markets[i].name) == 0) {
                    mpd_copy(token_rate, settings.markets[i].last, &mpd_ctx);
                }
            }
            mpd_copy(asset_rate, mpd_one, &mpd_ctx);
        } else {
            sds asset_cny = sdsempty();
            sdscpy(asset_cny, asset);
            asset_cny = sdscat(asset_cny, cny);
            if (get_market(asset_cny) == NULL)
                goto invalid_argument;

            for (size_t i = 0; i < settings.market_num; ++i) {
                if (strcmp(token_cny, settings.markets[i].name) == 0) {
                    mpd_copy(token_rate, settings.markets[i].last, &mpd_ctx);
                }
                if (strcmp(asset_cny, settings.markets[i].name) == 0) {
                    mpd_copy(asset_rate, settings.markets[i].last, &mpd_ctx);
                }
            }
        }
    }

    json_t *result = NULL;
    int ret = market_put_market_order(true, &result, market, user_id, side, amount, taker_fee, source,
                                      token, discount, token_rate, asset_rate);

    mpd_del(amount);
    mpd_del(taker_fee);
    mpd_del(discount);
    mpd_del(token_rate);
    mpd_del(asset_rate);

    if (ret == -1) {
        return reply_error(ses, pkg, 11, "balance not enough");
    } else if (ret == -2) {
        return reply_error(ses, pkg, 12, "amount too small");
    } else if (ret == -3) {
        return reply_error(ses, pkg, 13, "no enough trader");
    } else if (ret == -4) {
        return reply_error(ses, pkg, 17, "rate is zero");
    } else if (ret < 0) {
        log_fatal("market_put_limit_order fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }

    append_operlog("market_order", params);
    ret = reply_result(ses, pkg, result, true);
    json_decref(result);
    return ret;

invalid_argument:
    if (amount)
        mpd_del(amount);
    if (taker_fee)
        mpd_del(taker_fee);
    if (discount)
        mpd_del(discount);
    if (token_rate)
        mpd_del(token_rate);
    if (asset_rate)
        mpd_del(asset_rate);

    return reply_error_invalid_argument(ses, pkg);
}

static json_t *get_conversion_order(MYSQL *conn, uint32_t user_id, size_t offset, size_t limit)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "SELECT  `id`,`ctime`,`mtime`,`user`,`market`,`source`,`type`, `side`,`price`,`amount`,`taker_fee`,`maker_fee`,`money`,`stock`, `left` "
                            "from `orders` where `id` in (SELECT `deal_order_id` FROM "
                            "`user_deal_history_%u` where `user_id` = %u "
                       , user_id % HISTORY_HASH_NUM, user_id);

    sql = sdscatprintf(sql, " ) ORDER BY `id` DESC");
    if (limit) {
        if (offset) {
            sql = sdscatprintf(sql, " LIMIT %zu, %zu", offset, limit);
        } else {
            sql = sdscatprintf(sql, " LIMIT %zu", limit);
        }
    }

    log_trace("exec sql: %s", sql);

    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_fatal("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return NULL;
    }
    sdsfree(sql);

    MYSQL_RES *result = mysql_store_result(conn);
    size_t num_rows = mysql_num_rows(result);
    json_t *records = json_array();

    for (size_t i = 0; i < num_rows; ++i) {
        json_t *record = json_object();
        MYSQL_ROW row = mysql_fetch_row(result);
        uint64_t order_id = strtoull(row[0], NULL, 0);
        json_object_set_new(record, "id", json_integer(order_id));
        double ctime = strtod(row[1], NULL);
        json_object_set_new(record, "ctime", json_real(ctime));
        double ftime = strtod(row[2], NULL);
        json_object_set_new(record, "ftime", json_real(ftime));
        uint32_t user_id = strtoul(row[3], NULL, 0);
        json_object_set_new(record, "user", json_integer(user_id));
        json_object_set_new(record, "market", json_string(row[4]));
        json_object_set_new(record, "source", json_string(row[5]));
        uint32_t type = atoi(row[6]);
        json_object_set_new(record, "type", json_integer(type));
        uint32_t side = atoi(row[7]);
        json_object_set_new(record, "side", json_integer(side));
        json_object_set_new(record, "price", json_string(rstripzero(row[8])));
        json_object_set_new(record, "amount", json_string(rstripzero(row[9])));
        json_object_set_new(record, "taker_fee", json_string(rstripzero(row[10])));
        json_object_set_new(record, "maker_fee", json_string(rstripzero(row[10])));
        json_object_set_new(record, "stock", json_string(rstripzero(row[12])));
        json_object_set_new(record, "money", json_string(rstripzero(row[13])));
        json_object_set_new(record, "left", json_string(rstripzero(row[14])));
        json_array_append_new(records, record);
    }

    mysql_free_result(result);
    return records;
}

static int on_cmd_conversion_query(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));


    // offset
    if (!json_is_integer(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    size_t offset = json_integer_value(json_array_get(params, 1));

    // limit
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    size_t limit = json_integer_value(json_array_get(params, 2));
    if (limit > ORDER_LIST_MAX_LEN)
        return reply_error_invalid_argument(ses, pkg);

    //side
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    size_t  side = json_integer_value(json_array_get(params, 3));

    if (side < 0 || side > 2)
        return reply_error_invalid_argument(ses, pkg);

    json_t *result = json_object();
    json_object_set_new(result, "limit", json_integer(limit));
    json_object_set_new(result, "offset", json_integer(offset));
    json_t *orders = json_array();

    if (side == MARKET_ROLE_MAKER)
    {
        int  scan_cursor = 0;
        for (int i = 0; i < settings.market_num; ++i) {

            const char * market_name = settings.markets[i].name;
            market_t *market = get_market(market_name);
            if (market == NULL)
                continue;

            skiplist_t *order_list = market_get_order_list(market, user_id);

            if(order_list == NULL)
                continue;

            int sum_scan = scan_cursor + order_list->len;
            if (sum_scan < offset)
            {
                scan_cursor += order_list->len;
                continue;
            }
            skiplist_iter *iter = skiplist_get_iterator(order_list);
            skiplist_node *node;
            if (scan_cursor <= offset)
            {
                for(;scan_cursor < offset; scan_cursor++)
                {
                    if (skiplist_next(iter) == NULL)
                        break;
                }
            }

            size_t index = 0;
            while (scan_cursor < offset + limit && (node = skiplist_next(iter)) != NULL)
            {
                index ++;
                order_t *order = node->value;
                json_array_append_new(orders, get_order_info(order));
            }
            skiplist_release_iterator(iter);

            if (scan_cursor == offset + limit)
            {
                break;
            }
        }
    }
    else if( side == MARKET_ROLE_TAKER )
    {
        MYSQL *conn = mysql_connect(&settings.db_history);
        if (conn == NULL) {
            log_error("connect mysql fail");
            log_stderr("connect mysql fail");
            return reply_error_service_unavailable(ses,pkg);
        }
        else
        {
            orders = get_conversion_order(conn,user_id,offset,limit);
        }
    }

    json_object_set_new(result, "records", orders);
    int ret = reply_result(ses, pkg, result, false);
    json_decref(result);
    return ret;
}



static int on_cmd_order_query(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // offset
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    size_t offset = json_integer_value(json_array_get(params, 2));

    // limit
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    size_t limit = json_integer_value(json_array_get(params, 3));
    if (limit > ORDER_LIST_MAX_LEN)
        return reply_error_invalid_argument(ses, pkg);

    json_t *result = json_object();
    json_object_set_new(result, "limit", json_integer(limit));
    json_object_set_new(result, "offset", json_integer(offset));

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
                json_array_append_new(orders, get_order_info(order));
            }
            skiplist_release_iterator(iter);
        }
    }

    json_object_set_new(result, "records", orders);
    int ret = reply_result(ses, pkg, result, false);
    json_decref(result);
    return ret;
}

static int on_cmd_order_cancel(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 3)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // order_id
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t order_id = json_integer_value(json_array_get(params, 2));

    order_t *order = market_get_order(market, order_id);
    if (order == NULL) {
        return reply_error(ses, pkg, 14, "order not found");
    }
    if (order->user_id != user_id) {
        return reply_error(ses, pkg, 15, "user not match");
    }

    json_t *result = NULL;

    /*
    const char *cny = "CNY";
    sds token_cny = sdsempty();
    sdscpy(token_cny, token);
    token_cny = sdscat(token_cny, cny);
    */
    //check market
    sds volume = mpd_to_sci(order->left, 0);//sdsempty();
    sds stock_volume = sdsempty();
    const char *symbol = "-";
    sdscpy(stock_volume,symbol);
    stock_volume = sdscat(stock_volume,volume);

    if(order->side == MARKET_ORDER_SIDE_ASK)
    {
        mpd_t* result = balance_sub(order->user_id,BALANCE_TYPE_PLEDGE,market->stock,order->left);
        if(result == NULL)
        {
            return reply_error(ses, pkg, 110, "back stock failed");
        }
        balance_add(order->user_id,BALANCE_TYPE_AVAILABLE,market->stock,order->left);
    }
    else
    {
        mpd_t* result = balance_sub(order->user_id,BALANCE_TYPE_PLEDGE,market->money,order->left);
        if(result == NULL)
        {
            return reply_error(ses, pkg, 110, "back money failed");
        }
        balance_add(order->user_id,BALANCE_TYPE_AVAILABLE,market->money,order->left);
    }

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

#ifdef  ORDER_CANCEL_BATCH
static int on_cmd_order_cancel_batch(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params)  < 2)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    //market
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);

    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    skiplist_iter *iter = skiplist_get_iterator(market_get_order_list(market,user_id));
    skiplist_node *node;
    uint32_t count = 0;
    while ((node = skiplist_next(iter)) != NULL) {
        order_t *order = node->value;
        json_t *result = NULL;
        int ret = market_cancel_order(true, &result, market, order);
        if (ret < 0) {
            log_fatal("cancel order: %"PRIu64" fail: %d", order->id, ret);
            skiplist_release_iterator(iter);
            return reply_error_internal_error(ses, pkg);
        }
        append_operlog("cancel_order", params);
        json_decref(result);
        count ++;
        if (count == 999) {
            break;
        }
    }
    skiplist_release_iterator(iter);
    return reply_success(ses,pkg);
}
#endif

static int on_cmd_order_book(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return reply_error_invalid_argument(ses, pkg);

    // market
    if (!json_is_string(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 0));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // side
    if (!json_is_integer(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 1));
    if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return reply_error_invalid_argument(ses, pkg);

    // offset
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    size_t offset = json_integer_value(json_array_get(params, 2));

    // limit
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    size_t limit = json_integer_value(json_array_get(params, 3));
    if (limit > ORDER_BOOK_MAX_LEN)
        return reply_error_invalid_argument(ses, pkg);

    json_t *result = json_object();
    json_object_set_new(result, "offset", json_integer(offset));
    json_object_set_new(result, "limit", json_integer(limit));

    uint64_t total;
    skiplist_iter *iter;
    if (side == MARKET_ORDER_SIDE_ASK) {
        iter = skiplist_get_iterator(market->asks);
        total = market->asks->len;
        json_object_set_new(result, "total", json_integer(total));
    } else {
        iter = skiplist_get_iterator(market->bids);
        total = market->bids->len;
        json_object_set_new(result, "total", json_integer(total));
    }

    json_t *orders = json_array();
    if (offset < total) {
        for (size_t i = 0; i < offset; i++) {
            if (skiplist_next(iter) == NULL)
                break;
        }
        size_t index = 0;
        skiplist_node *node;
        while ((node = skiplist_next(iter)) != NULL && index < limit) {
            index++;
            order_t *order = node->value;
            json_array_append_new(orders, get_order_info(order));
        }
    }
    skiplist_release_iterator(iter);

    json_object_set_new(result, "orders", orders);
    int ret = reply_result(ses, pkg, result, false);
    json_decref(result);
    return ret;
}

static json_t *get_depth(market_t *market, size_t limit)
{
    mpd_t *price = mpd_new(&mpd_ctx);
    mpd_t *amount = mpd_new(&mpd_ctx);

    json_t *asks = json_array();
    skiplist_iter *iter = skiplist_get_iterator(market->asks);
    skiplist_node *node = skiplist_next(iter);
    size_t index = 0;
    while (node && index < limit) {
        index++;
        order_t *order = node->value;
        mpd_copy(price, order->price, &mpd_ctx);
        mpd_copy(amount, order->left, &mpd_ctx);
        while ((node = skiplist_next(iter)) != NULL) {
            order = node->value;
            if (mpd_cmp(price, order->price, &mpd_ctx) == 0) {
                mpd_add(amount, amount, order->left, &mpd_ctx);
            } else {
                break;
            }
        }
        json_t *info = json_array();
        json_array_append_new_mpd(info, price);
        json_array_append_new_mpd(info, amount);
        json_array_append_new(asks, info);
    }
    skiplist_release_iterator(iter);

    json_t *bids = json_array();
    iter = skiplist_get_iterator(market->bids);
    node = skiplist_next(iter);
    index = 0;
    while (node && index < limit) {
        index++;
        order_t *order = node->value;
        mpd_copy(price, order->price, &mpd_ctx);
        mpd_copy(amount, order->left, &mpd_ctx);
        while ((node = skiplist_next(iter)) != NULL) {
            order = node->value;
            if (mpd_cmp(price, order->price, &mpd_ctx) == 0) {
                mpd_add(amount, amount, order->left, &mpd_ctx);
            } else {
                break;
            }
        }
        json_t *info = json_array();
        json_array_append_new_mpd(info, price);
        json_array_append_new_mpd(info, amount);
        json_array_append_new(bids, info);
    }
    skiplist_release_iterator(iter);

    mpd_del(price);
    mpd_del(amount);

    json_t *result = json_object();
    json_object_set_new(result, "asks", asks);
    json_object_set_new(result, "bids", bids);

    return result;
}

static json_t *get_market_depth(market_t *market)
{
    mpd_t *price = mpd_new(&mpd_ctx);
    json_t *result = json_object();

    skiplist_iter *iter = skiplist_get_iterator(market->asks);
    skiplist_node *node = skiplist_next(iter);
    if (node) {
        order_t *order = node->value;
        mpd_copy(price, order->price, &mpd_ctx);
        json_object_set_new_mpd(result, "ask", price);
    } else {
        json_object_set_new(result, "ask", json_string("0"));
    }
    skiplist_release_iterator(iter);

    iter = skiplist_get_iterator(market->bids);
    node = skiplist_next(iter);
    if (node) {
        order_t *order = node->value;
        mpd_copy(price, order->price, &mpd_ctx);
        json_object_set_new_mpd(result, "bid", price);
    } else {
        json_object_set_new(result, "bid", json_string("0"));
    }
    skiplist_release_iterator(iter);

    mpd_del(price);

    return result;
}

static json_t *get_depth_merge(market_t* market, size_t limit, mpd_t *interval)
{
    mpd_t *q = mpd_new(&mpd_ctx);
    mpd_t *r = mpd_new(&mpd_ctx);
    mpd_t *price = mpd_new(&mpd_ctx);
    mpd_t *amount = mpd_new(&mpd_ctx);

    json_t *asks = json_array();
    skiplist_iter *iter = skiplist_get_iterator(market->asks);
    skiplist_node *node = skiplist_next(iter);
    size_t index = 0;
    while (node && index < limit) {
        index++;
        order_t *order = node->value;
        mpd_divmod(q, r, order->price, interval, &mpd_ctx);
        mpd_mul(price, q, interval, &mpd_ctx);
        if (mpd_cmp(r, mpd_zero, &mpd_ctx) != 0) {
            mpd_add(price, price, interval, &mpd_ctx);
        }
        mpd_copy(amount, order->left, &mpd_ctx);
        while ((node = skiplist_next(iter)) != NULL) {
            order = node->value;
            if (mpd_cmp(price, order->price, &mpd_ctx) >= 0) {
                mpd_add(amount, amount, order->left, &mpd_ctx);
            } else {
                break;
            }
        }
        json_t *info = json_array();
        json_array_append_new_mpd(info, price);
        json_array_append_new_mpd(info, amount);
        json_array_append_new(asks, info);
    }
    skiplist_release_iterator(iter);

    json_t *bids = json_array();
    iter = skiplist_get_iterator(market->bids);
    node = skiplist_next(iter);
    index = 0;
    while (node && index < limit) {
        index++;
        order_t *order = node->value;
        mpd_divmod(q, r, order->price, interval, &mpd_ctx);
        mpd_mul(price, q, interval, &mpd_ctx);
        mpd_copy(amount, order->left, &mpd_ctx);
        while ((node = skiplist_next(iter)) != NULL) {
            order = node->value;
            if (mpd_cmp(price, order->price, &mpd_ctx) <= 0) {
                mpd_add(amount, amount, order->left, &mpd_ctx);
            } else {
                break;
            }
        }

        json_t *info = json_array();
        json_array_append_new_mpd(info, price);
        json_array_append_new_mpd(info, amount);
        json_array_append_new(bids, info);
    }
    skiplist_release_iterator(iter);

    mpd_del(q);
    mpd_del(r);
    mpd_del(price);
    mpd_del(amount);

    json_t *result = json_object();
    json_object_set_new(result, "asks", asks);
    json_object_set_new(result, "bids", bids);

    return result;
}

static int on_cmd_order_book_depth(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 3)
        return reply_error_invalid_argument(ses, pkg);

    // market
    if (!json_is_string(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 0));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // limit
    if (!json_is_integer(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    size_t limit = json_integer_value(json_array_get(params, 1));
    if (limit > ORDER_BOOK_MAX_LEN)
        return reply_error_invalid_argument(ses, pkg);

    // interval
    if (!json_is_string(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    mpd_t *interval = decimal(json_string_value(json_array_get(params, 2)), market->money_prec);
    if (!interval)
        return reply_error_invalid_argument(ses, pkg);
    if (mpd_cmp(interval, mpd_zero, &mpd_ctx) < 0) {
        mpd_del(interval);
        return reply_error_invalid_argument(ses, pkg);
    }
    sds cache_key = NULL;
    if (process_cache(ses, pkg, &cache_key)) {
        mpd_del(interval);
        return 0;
    }

    json_t *result = NULL;
    if (mpd_cmp(interval, mpd_zero, &mpd_ctx) == 0) {
        result = get_depth(market, limit);
    } else {
        result = get_depth_merge(market, limit, interval);
    }
    mpd_del(interval);

    if (result == NULL) {
        sdsfree(cache_key);
        return reply_error_internal_error(ses, pkg);
    }

    add_cache(cache_key, result);
    sdsfree(cache_key);

    int ret = reply_result(ses, pkg, result, false);
    json_decref(result);
    return ret;
}

static int on_cmd_market_depth(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{   
    if (json_array_size(params) != 0)
        return reply_error_invalid_argument(ses, pkg);
    
    const char *CNY = "CNY";
    const char *BTC = "BTC";
    const char *ETH = "ETH";
    const char *BCH = "BCH";
    json_t *result = json_array();

    sds cache_key = NULL;
    if (process_cache(ses, pkg, &cache_key)) {
        return 0;
    }
    
    for (int i = 0; i < settings.market_num; ++i) {
        const char *money = settings.markets[i].money;
        if (strcmp(money, CNY) == 0 || strcmp(money, BTC) == 0 || strcmp(money, ETH) == 0 || strcmp(money, BCH) == 0) {
            const char *name = settings.markets[i].name;
            market_t *market = get_market(name);
            json_t *entry = get_market_depth(market);
            if (entry == NULL) {
                sdsfree(cache_key);
                return reply_error_internal_error(ses, pkg);
            }
            json_object_set_new(entry, "market", json_string(name));
            json_array_append_new(result, entry);
        }
    }
    
    if (result == NULL) {
        sdsfree(cache_key);
        return reply_error_internal_error(ses, pkg);
    }

    add_cache(cache_key, result);
    sdsfree(cache_key);
    
    int ret = reply_result(ses, pkg, result, false);
    json_decref(result);
    return ret;
}

static int on_cmd_order_detail(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 2)
        return reply_error_invalid_argument(ses, pkg);

    // market
    if (!json_is_string(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 0));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // order_id
    if (!json_is_integer(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t order_id = json_integer_value(json_array_get(params, 1));

    order_t *order = market_get_order(market, order_id);
    json_t *result = NULL;
    if (order == NULL) {
        result = json_null();
    } else {
        result = get_order_info(order);
    }

    int ret = reply_result(ses, pkg, result, false);
    json_decref(result);
    return ret;
}

static int on_cmd_market_list(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    json_t *result = json_array();
    for (int i = 0; i < settings.market_num; ++i) {
        json_t *market = json_object();
        json_object_set_new(market, "name", json_string(settings.markets[i].name));
        json_object_set_new(market, "stock", json_string(settings.markets[i].stock));
        json_object_set_new(market, "money", json_string(settings.markets[i].money));
        json_object_set_new(market, "fee_prec", json_integer(settings.markets[i].fee_prec));
        json_object_set_new(market, "stock_prec", json_integer(settings.markets[i].stock_prec));
        json_object_set_new(market, "money_prec", json_integer(settings.markets[i].stock_prec));
        json_object_set_new_mpd(market, "min_amount", settings.markets[i].min_amount);
        json_array_append_new(result, market);
    }

    int ret = reply_result(ses, pkg, result, false);
    json_decref(result);
    return ret;
}

static json_t *get_market_summary(const char *name)
{
    size_t ask_count;
    size_t bid_count;
    mpd_t *ask_amount = mpd_new(&mpd_ctx);
    mpd_t *bid_amount = mpd_new(&mpd_ctx);
    market_t *market = get_market(name);
    market_get_status(market, &ask_count, ask_amount, &bid_count, bid_amount);
    
    json_t *obj = json_object();
    json_object_set_new(obj, "name", json_string(name));
    json_object_set_new(obj, "ask_count", json_integer(ask_count));
    json_object_set_new_mpd(obj, "ask_amount", ask_amount);
    json_object_set_new(obj, "bid_count", json_integer(bid_count));
    json_object_set_new_mpd(obj, "bid_amount", bid_amount);

    mpd_del(ask_amount);
    mpd_del(bid_amount);

    return obj;
}

static int on_cmd_market_summary(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    json_t *result = json_array();
    if (json_array_size(params) == 0) {
        for (int i = 0; i < settings.market_num; ++i) {
            json_array_append_new(result, get_market_summary(settings.markets[i].name));
        }
    } else {
        for (int i = 0; i < json_array_size(params); ++i) {
            const char *market = json_string_value(json_array_get(params, i));
            if (market == NULL)
                goto invalid_argument;
            if (get_market(market) == NULL)
                goto invalid_argument;
            json_array_append_new(result, get_market_summary(market));
        }
    }

    int ret = reply_result(ses, pkg, result, false);
    json_decref(result);
    return ret;

invalid_argument:
    json_decref(result);
    return reply_error_invalid_argument(ses, pkg);
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
    case CMD_BALANCE_QUERY:
        log_trace("from: %s cmd balance query, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_balance_query(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_balance_query %s fail: %d", params_str, ret);
        }
        break;
    case CMD_BALANCE_UPDATE:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                      is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }

        log_trace("from: %s cmd balance update, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_balance_update(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_balance_update %s fail: %d", params_str, ret);
        }
        break;
#ifdef FREEZE_BALANCE
    case CMD_BALANCE_FREEZE:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                      is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd balance freeze, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_balance_freeze(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_balance_freeze %s fail: %d", params_str, ret);
        }
        break;

    case CMD_PLEDGE_WITHDRAW:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                      is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd balance deposit, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_balance_withdraw(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_balance_withdraw %s fail: %d", params_str, ret);
        }
        break;
    case CMD_BALANCE_SETTLE:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                      is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd balance freeze, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_balance_settle(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_balance_settle %s fail: %d", params_str, ret);
        }
        break;
    case CMD_BALANCE_RELEASE:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                      is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd balance freeze, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_balance_release(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_balance_release %s fail: %d", params_str, ret);
        }
        break;
    case CMD_BALANCE_EXCEPTION:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                      is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd balance freeze, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_balance_exception(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_balance_exception %s fail: %d", params_str, ret);
        }
        break;
    case CMD_BALANCE_REWARDS:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                      is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd balance rewards, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_balance_reward(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_balance_reward %s fail: %d", params_str, ret);
        }
        break;
    case CMD_BALANCE_AIRDROP:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                      is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd balance airdrop, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_balance_airdrop(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_balance_airdrop %s fail: %d", params_str, ret);
        }
        break;

    case CMD_BALANCE_ADDNEGACTIVE:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                      is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd balance add negtive, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_balance_addnegactive(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_balance_addnegactive %s fail: %d", params_str, ret);
        }
        break;
#endif
//#ifdef CONVERSION
    case CMD_ORDER_PUT_ORDER:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                      is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s put order, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_put_order(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_put_order %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_PUT_MATCH:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                      is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s put match, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_put_match(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_put_match %s fail: %d", params_str, ret);
        }
        break;
//#endif
    case CMD_ASSET_LIST:
        log_trace("from: %s cmd asset list, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_asset_list(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_asset_list %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ASSET_SUMMARY:
        log_trace("from: %s cmd asset summary, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_asset_summary(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_asset_summary %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_PUT_LIMIT:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                      is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd order put limit, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_put_limit(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_put_limit %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_PUT_MARKET:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                      is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd order put market, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_put_market(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_put_market %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_QUERY:
        log_trace("from: %s cmd order query, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_query(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_query %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_CANCEL:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                      is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd order cancel, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_cancel(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_cancel %s fail: %d", params_str, ret);
        }
        break;
#ifdef ORDER_CANCEL_BATCH
    case CMD_ORDER_CANCEL_BATCH:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                      is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd order cancel batch, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_cancel_batch(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_cancel batch %s fail: %d", params_str, ret);
        }
        break;
#endif
    case CMD_ORDER_BOOK:
        log_trace("from: %s cmd order book, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_book(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_book %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_BOOK_DEPTH:
        log_trace("from: %s cmd order book depth, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_book_depth(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_book_depth %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_DETAIL:
        log_trace("from: %s cmd order detail, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_detail(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_detail %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MARKET_LIST:
        log_trace("from: %s cmd market list, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_market_list(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_market_list %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MARKET_SUMMARY:
        log_trace("from: %s cmd market summary, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_market_summary(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_market_summary%s fail: %d", params_str, ret);
        }
        break;

    case CMD_MARKET_UPDATE:
        log_trace("from: %s cmd market update, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_market_update(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_market_update %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ASSET_UPDATE:
        log_trace("from: %s cmd asset update, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_asset_update(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_asset_update %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MARKET_DEPTH:
        log_trace("from: %s cmd market depth, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_market_depth(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_market_depth %s fail: %d", params_str, ret);
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

