/*
 * Description:
 *     History: yang@haipo.me, 2017/03/16, create
 */

# include <curl/curl.h>

# include "me_config.h"
# include "me_market.h"
# include "me_history.h"
# include "me_balance.h"

uint64_t order_id_start;
uint64_t deals_id_start;

struct dict_user_key {
    uint32_t    user_id;
};

struct dict_order_key {
    uint64_t    order_id;
};

static uint32_t dict_user_hash_function(const void *key)
{
    const struct dict_user_key *obj = key;
    return obj->user_id;
}

static int dict_user_key_compare(const void *key1, const void *key2)
{
    const struct dict_user_key *obj1 = key1;
    const struct dict_user_key *obj2 = key2;
    if (obj1->user_id == obj2->user_id) {
        return 0;
    }
    return 1;
}

static void *dict_user_key_dup(const void *key)
{
    struct dict_user_key *obj = malloc(sizeof(struct dict_user_key));
    memcpy(obj, key, sizeof(struct dict_user_key));
    return obj;
}

static void dict_user_key_free(void *key)
{
    free(key);
}

static void dict_user_val_free(void *key)
{
    skiplist_release(key);
}

static uint32_t dict_order_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct dict_order_key));
}

static int dict_order_key_compare(const void *key1, const void *key2)
{
    const struct dict_order_key *obj1 = key1;
    const struct dict_order_key *obj2 = key2;
    if (obj1->order_id == obj2->order_id) {
        return 0;
    }
    return 1;
}

static void *dict_order_key_dup(const void *key)
{
    struct dict_order_key *obj = malloc(sizeof(struct dict_order_key));
    memcpy(obj, key, sizeof(struct dict_order_key));
    return obj;
}

static void dict_order_key_free(void *key)
{
    free(key);
}

static int order_match_compare(const void *value1, const void *value2)
{
    const order_t *order1 = value1;
    const order_t *order2 = value2;

    if (order1->id == order2->id) {
        return 0;
    }
    if (order1->type != order2->type) {
        return 1;
    }

    int cmp;
    if (order1->side == MARKET_ORDER_SIDE_ASK) {
    } else {
    }
    if (cmp != 0) {
        return cmp;
    }

    return order1->id > order2->id ? 1 : -1;
}

static int order_id_compare(const void *value1, const void *value2)
{
    const order_t *order1 = value1;
    const order_t *order2 = value2;
    if (order1->id == order2->id) {
        return 0;
    }

    return order1->id > order2->id ? -1 : 1;
}

static void order_free(order_t *order)
{
    free(order->asset);
    free(order->market);
    free(order->amounts);
    free(order->uids);
    free(order->times);
    free(order);
}

// envelope
json_t *get_order_info(order_t *order, int pos)
{
    json_t *info = json_object();
    json_object_set_new(info, "id", json_integer(order->id));
    json_object_set_new(info, "type", json_integer(order->type));
    json_object_set_new(info, "user", json_integer(order->user_id));
    json_object_set_new(info, "time", json_real(order->create_time));
    json_object_set_new(info, "asset", json_string(order->asset));

    char str_supply[20] = {0};
    sprintf(str_supply, "%f", order->supply);
    json_object_set_new(info, "supply", json_string(str_supply));
    char str_leave[20] = {0};
    sprintf(str_leave, "%f", order->leave);
    json_object_set_new(info, "leave", json_real(order->leave));

    json_object_set_new(info, "share", json_integer(order->share));
    json_object_set_new(info, "expire_time", json_integer(order->expire_time));
    json_object_set_new(info, "count", json_integer(order->count));

    json_t *history = json_array();
    for (int i = 0; i < order->count; ++i) {
        json_t *item = json_object();
        json_t *uids = json_loads(order->uids, 0, NULL);
        json_t *amounts = json_loads(order->amounts, 0, NULL);
        json_t *times = json_loads(order->times, 0, NULL);
        json_object_set_new(item, "uid", json_array_get(uids, i));
        json_object_set_new(item, "amount", json_array_get(amounts, i));
        json_object_set_new(item, "time", json_array_get(times, i));

        if (pos != -1 && pos == i)
            json_object_set_new(info, "amount", json_array_get(amounts, i));

        json_array_append_new(history, item);
    }
    json_object_set_new(info, "history", history);

    return info;
}

static int order_put(market_t *m, order_t *order)
{
    struct dict_order_key order_key = { .order_id = order->id };
    if (dict_add(m->orders, &order_key, order) == NULL)
        return -__LINE__;

    struct dict_user_key user_key = { .user_id = order->user_id };
    dict_entry *entry = dict_find(m->users, &user_key);
    if (entry) {
        skiplist_t *order_list = entry->val;
        if (skiplist_insert(order_list, order) == NULL)
            return -__LINE__;
    } else {
        skiplist_type type;
        memset(&type, 0, sizeof(type));
        type.compare = order_id_compare;
        skiplist_t *order_list = skiplist_create(&type);
        if (order_list == NULL)
            return -__LINE__;
        if (skiplist_insert(order_list, order) == NULL)
            return -__LINE__;
        if (dict_add(m->users, &user_key, order_list) == NULL)
            return -__LINE__;
    }

    if (skiplist_insert(m->asks, order) == NULL)
        return -__LINE__;

    return 0;
}

static int order_finish(bool real, market_t *m, order_t *order)
{
    skiplist_node *node = skiplist_find(m->asks, order);
    if (node) {
        skiplist_delete(m->asks, node);
    }

    struct dict_order_key order_key = { .order_id = order->id };
    dict_delete(m->orders, &order_key);

    struct dict_user_key user_key = { .user_id = order->user_id };
    dict_entry *entry = dict_find(m->users, &user_key);
    if (entry) {
        skiplist_t *order_list = entry->val;
        skiplist_node *node = skiplist_find(order_list, order);
        if (node) {
            skiplist_delete(order_list, node);
        }
    }

    order_free(order);
    return 0;
}

market_t *market_create(struct market *conf)
{
    if (!asset_exist(conf->stock) || !asset_exist(conf->money))
        return NULL;
    if (conf->stock_prec + conf->money_prec > asset_prec(conf->money))
        return NULL;
    if (conf->stock_prec + conf->fee_prec > asset_prec(conf->stock))
        return NULL;
    if (conf->money_prec + conf->fee_prec > asset_prec(conf->money))
        return NULL;

    market_t *m = malloc(sizeof(market_t));
    memset(m, 0, sizeof(market_t));
    m->name             = strdup(conf->name);
    m->stock            = strdup(conf->stock);
    m->money            = strdup(conf->money);
    m->stock_prec       = conf->stock_prec;
    m->money_prec       = conf->money_prec;
    m->fee_prec         = conf->fee_prec;

    mpd_t *amount = mpd_new(&mpd_ctx);
    mpd_set_string(amount, "0.001", &mpd_ctx);
    m->min_amount  = amount;

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function    = dict_user_hash_function;
    dt.key_compare      = dict_user_key_compare;
    dt.key_dup          = dict_user_key_dup;
    dt.key_destructor   = dict_user_key_free;
    dt.val_destructor   = dict_user_val_free;

    m->users = dict_create(&dt, 1024);
    if (m->users == NULL)
        return NULL;

    memset(&dt, 0, sizeof(dt));
    dt.hash_function    = dict_order_hash_function;
    dt.key_compare      = dict_order_key_compare;
    dt.key_dup          = dict_order_key_dup;
    dt.key_destructor   = dict_order_key_free;

    m->orders = dict_create(&dt, 1024);
    if (m->orders == NULL)
        return NULL;

    skiplist_type lt;
    memset(&lt, 0, sizeof(lt));
    lt.compare          = order_match_compare;

    m->asks = skiplist_create(&lt);
    m->bids = skiplist_create(&lt);
    if (m->asks == NULL || m->bids == NULL)
        return NULL;

    return m;
}


int market_put_order(market_t *m, order_t *order)
{
    return order_put(m, order);
}

order_t *market_get_order(market_t *m, uint64_t order_id)
{
    struct dict_order_key key = { .order_id = order_id };
    dict_entry *entry = dict_find(m->orders, &key);
    if (entry) {
        return entry->val;
    }
    return NULL;
}

skiplist_t *market_get_order_list(market_t *m, uint32_t user_id)
{
    struct dict_user_key key = { .user_id = user_id };
    dict_entry *entry = dict_find(m->users, &key);
    if (entry) {
        return entry->val;
    }
    return NULL;
}

// envelope
static size_t post_write_callback(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    sds *reply = userdata;
    *reply = sdscatlen(*reply, ptr, size * nmemb);
    return size * nmemb;
}

static uint32_t send_balance_req(json_t* request)
{
    json_t *reply  = NULL;
    json_t *error  = NULL;
    uint32_t error_code = 0;

    char *request_data = json_dumps(request, 0);

    CURL *curl = curl_easy_init();
    sds reply_str = sdsempty();

    struct curl_slist *chunk = NULL;
    chunk = curl_slist_append(chunk, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
    curl_easy_setopt(curl, CURLOPT_URL, settings.accesshttp);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, post_write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &reply_str);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, (long)(1000));
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request_data);

    CURLcode ret = curl_easy_perform(curl);
    if (ret != CURLE_OK) {
        error_code = -1;
        log_fatal("curl_easy_perform fail: %s", curl_easy_strerror(ret));
        goto cleanup;
    }

    reply = json_loads(reply_str, 0, NULL);
    if (reply == NULL)
        goto cleanup;
    error = json_object_get(reply, "error");
    if (!json_is_null(error)) {
        error_code = json_integer_value(json_object_get(error, "code"));
        log_error("send balance req fail: %s", reply_str);
        goto cleanup;
    }

cleanup:
    free(request_data);
    sdsfree(reply_str);
    curl_easy_cleanup(curl);
    curl_slist_free_all(chunk);
    if (reply)
        json_decref(reply);

    return error_code;
}

// balance.freeze (uid, asset, business type, business_id, change, detail)
static uint32_t balance_freeze_req(uint32_t user_id, const char *asset, const char *change, uint64_t envelope_id)
{
    json_t *request = json_object();
    json_t *request_params = json_array();
    json_array_append_new(request_params, json_integer(user_id));
    json_array_append_new(request_params, json_string(asset));
    json_array_append_new(request_params, json_string("envelope"));
    json_array_append_new(request_params, json_integer(time(NULL)));
    json_array_append_new(request_params, json_string(change));
    json_t *detail = json_object();
    json_object_set_new(detail, "envelope_id", json_integer(envelope_id));
    json_object_set_new(detail, "action", json_integer(BALANCE_ACTION_PUT));
    json_array_append_new(request_params, detail);
    json_object_set_new(request, "method", json_string("balance.freeze"));
    json_object_set_new(request, "params", request_params);
    json_object_set_new(request, "id", json_integer(time(NULL)));
    return send_balance_req(request);
}

static uint32_t balance_unfreeze_req(uint32_t user_id, const char *asset, const char *change, uint64_t envelope_id, uint16_t action)
{
    json_t *request = json_object();
    json_t *request_params = json_array();
    json_array_append_new(request_params, json_integer(user_id));
    json_array_append_new(request_params, json_string(asset));
    json_array_append_new(request_params, json_string("envelope"));
    json_array_append_new(request_params, json_integer(time(NULL)));

    sds balance = sdsempty();
    sdscpy(balance, "-");
    balance = sdscat(balance, change);

    json_array_append_new(request_params, json_string(balance));
    json_t *detail = json_object();
    json_object_set_new(detail, "envelope_id", json_integer(envelope_id));
    json_object_set_new(detail, "action", json_integer(action));
    json_array_append_new(request_params, detail);
    json_object_set_new(request, "method", json_string("balance.freeze"));
    json_object_set_new(request, "params", request_params);
    json_object_set_new(request, "id", json_integer(time(NULL)));
    return send_balance_req(request);
}

static uint32_t balance_unfreeze_double_req(uint32_t user_id, const char *asset, double change, uint64_t envelope_id, uint16_t action)
{
    json_t *request = json_object();
    json_t *request_params = json_array();
    json_array_append_new(request_params, json_integer(user_id));
    json_array_append_new(request_params, json_string(asset));
    json_array_append_new(request_params, json_string("envelope"));
    json_array_append_new(request_params, json_integer(time(NULL)));

    char str_leave[20] = {0};
    sprintf(str_leave, "%.8f", change);

    sds balance = sdsempty();
    sdscpy(balance, "-");
    balance = sdscat(balance, str_leave);

    json_array_append_new(request_params, json_string(balance));
    json_t *detail = json_object();
    json_object_set_new(detail, "envelope_id", json_integer(envelope_id));
    json_object_set_new(detail, "action", json_integer(action));
    json_array_append_new(request_params, detail);
    json_object_set_new(request, "method", json_string("balance.freeze"));
    json_object_set_new(request, "params", request_params);
    json_object_set_new(request, "id", json_integer(time(NULL)));
    return send_balance_req(request);
}

static uint32_t balance_withdraw_req(uint32_t user_id, const char *asset, const char *change, uint64_t envelope_id)
{
    json_t *request = json_object();
    json_t *request_params = json_array();
    json_array_append_new(request_params, json_integer(user_id));
    json_array_append_new(request_params, json_string(asset));
    json_array_append_new(request_params, json_string("envelope"));
    json_array_append_new(request_params, json_integer(time(NULL)));

    sds balance = sdsempty();
    sdscpy(balance, "-");
    balance = sdscat(balance, change);

    json_array_append_new(request_params, json_string(balance));
    json_t *detail = json_object();
    json_object_set_new(detail, "envelope_id", json_integer(envelope_id));
    json_object_set_new(detail, "action", json_integer(BALANCE_ACTION_OPEN));
    json_array_append_new(request_params, detail);
    json_object_set_new(request, "method", json_string("balance.withdraw"));
    json_object_set_new(request, "params", request_params);
    json_object_set_new(request, "id", json_integer(time(NULL)));
    return send_balance_req(request);
}

// balance.freeze (uid, asset, business type, business_id, change, detail)
static bool balance_update_req(uint32_t user_id, const char *asset, const char *change, uint64_t envelope_id)
{
    json_t *request = json_object();
    json_t *request_params = json_array();
    json_array_append_new(request_params, json_integer(user_id));
    json_array_append_new(request_params, json_string(asset));
    json_array_append_new(request_params, json_string("envelope"));
    json_array_append_new(request_params, json_integer(time(NULL)));
    json_array_append_new(request_params, json_string(change));
    json_t *detail = json_object();
    json_object_set_new(detail, "envelope_id", json_integer(envelope_id));
    json_object_set_new(detail, "action", json_integer(BALANCE_ACTION_OPEN));
    json_array_append_new(request_params, detail);
    json_object_set_new(request, "method", json_string("balance.update"));
    json_object_set_new(request, "params", request_params);
    json_object_set_new(request, "id", json_integer(time(NULL)));
    return send_balance_req(request);
}

int market_cancel_order(bool real, json_t **result, market_t *m, order_t *order)
{
    if (real) {
        *result = get_order_info(order, -1);
     }

     // count != share means envelope expired
     if (real && order->count != order->share) {
        double current_time = current_timestamp();
        int res = balance_unfreeze_double_req(order->user_id, order->asset, order->leave, order->id, BALANCE_ACTION_EXPIRE);
        if (res != 0) {
            log_error("market_cancel_order fail");
             return -__LINE__;
        }

        res = append_user_envelope_history(current_time, order->user_id, order->asset, order->id, MARKET_ROLE_EXPIRE, order->leave);
        if (res < 0) {
            log_fatal("append_user_envelope_history fail: %d, envelope_id: %"PRIu64"", res, order->id);
            return -__LINE__;
        }
    }

    order_finish(real, m, order);
    return 0;
}

int envelope_put(bool real, json_t **result, market_t *m, uint32_t user_id, const char *asset, const char *supply,
                        uint32_t share, uint32_t type, uint32_t expire_time, double create_time)
{
    int ret = 0;
    if (real) {
        ret = balance_freeze_req(user_id, asset, supply, order_id_start + 1);
        if (ret != 0) {
            log_error("balance freeze request error");
            return ret;
        }
    }

    order_t *order = malloc(sizeof(order_t));
    if (order == NULL) {
        return -__LINE__;
    }

    order->id           = ++order_id_start;
    order->side         = 1;
    order->create_time  = create_time;
    order->market       = strdup(m->name);
    order->user_id      = user_id;
    order->asset        = strdup(asset);
    order->supply       = atof(supply);
    order->share        = share;
    order->type         = type;
    order->leave        = atof(supply);
    order->expire_time  = expire_time;
    order->count        = 0;
    order->amounts      = json_dumps(json_object(), 0);
    order->uids         = json_dumps(json_object(), 0);
    order->times        = json_dumps(json_object(), 0);

    double left = atof(supply);
    double temp = 0;

    json_t *json_amounts = json_array();

    if (type == ENVELOPE_TYPE_AVERAGE) {
        temp = atof(supply) / (double)share;

        char str_temp[24] = {0};
        sprintf(str_temp, "%.8f", temp);

        char str_share[24] = {0};
        sprintf(str_share, "%d", share - 1);

        mpd_t *supply_mp = NULL;
        mpd_t *temp_mp = NULL;
        mpd_t *share_mp = NULL;
        mpd_t *left_mp = mpd_new(&mpd_ctx);

        supply_mp = decimal(supply, 8);
        temp_mp = decimal(str_temp, 8);
        share_mp = decimal(str_share, 8);
        mpd_mul(temp_mp, temp_mp, share_mp, &mpd_ctx);
        mpd_sub(left_mp, supply_mp, temp_mp, &mpd_ctx);

        mpd_rescale(left_mp, left_mp, -8, &mpd_ctx);
        char *str_left = mpd_to_sci(left_mp, 0);

        if (supply_mp)
            mpd_del(supply_mp);
        if (supply_mp)
            mpd_del(temp_mp);
        if (supply_mp)
            mpd_del(left_mp);
        if (supply_mp)
            mpd_del(share_mp);

        for (size_t i = 0; i < share; ++i)
        {
            if (i == 0) {
                json_array_append_new(json_amounts, json_string(str_left));
            } else {
                json_array_append_new(json_amounts, json_string(str_temp));
            }
        }
    } else {

        mpd_t *left_mp = NULL;
        left_mp = decimal(supply, 8);
        mpd_t *temp_mp = mpd_new(&mpd_ctx);

        for (size_t i = share; i > 1; --i)
        {
            srand((unsigned)(create_time));
            double deno = rand() / (double)(RAND_MAX / share) + 1.0;

            char str_deno[24] = {0};
            sprintf(str_deno, "%.8f", deno);
            mpd_t *deno_mp = NULL;
            deno_mp = decimal(str_deno, 8);

            mpd_div(temp_mp, left_mp, deno_mp, &mpd_ctx);
            mpd_rescale(temp_mp, temp_mp, -8, &mpd_ctx);
            mpd_sub(left_mp, left_mp, temp_mp, &mpd_ctx);
            char *str_temp = mpd_to_sci(temp_mp, 0);
            json_array_append_new(json_amounts, json_string(str_temp));

            if (deno_mp)
                mpd_del(deno_mp);
        }

        char *str_left = mpd_to_sci(left_mp, 0);
        json_array_append_new(json_amounts, json_string(str_left));

        if (left_mp)
            mpd_del(left_mp);
        if (temp_mp)
            mpd_del(temp_mp);
    }

    order->amounts = json_dumps(json_amounts, 0);

    if (real) {
        ret = append_user_envelope_history(order->create_time, user_id, asset, order->id, MARKET_ROLE_MAKER, order->supply);
        if (ret < 0) {
            log_fatal("append_user_envelope_history fail: %d, envelope_id: %"PRIu64"", ret, order->id);
        }

        ret = append_envelope_detail(order->create_time, order->id, user_id, asset, type, supply, share, expire_time);
        if (ret < 0) {
            log_fatal("append_envelope_detail fail: %d, envelope_id: %"PRIu64"", ret, order->id);
        }

        *result = get_order_info(order, -1);
    }

    ret = order_put(m, order);
    if (ret < 0) {
        log_fatal("order_put fail: %d, order: %"PRIu64"", ret, order->id);
    }

    return 0;
}


int envelope_open(bool real, json_t **result, market_t *m, uint32_t user_id, order_t *order)
{
    // envelope expired, unfreeze balance
    double current_time = current_timestamp();
    /*
    if (current_time - order->create_time >= 3600 * order->expire_time) {
        char str_leave[10] = {0};
        sprintf(str_leave, "%.8f", order->leave);

        if (real) {
            int res = balance_unfreeze_req(order->user_id, order->asset, str_leave, order->id, BALANCE_ACTION_EXPIRE);
            if (res != 0) {
                log_error("balance unfreeze request error");
                return res;
            }

            res = append_user_envelope_history(current_time, user_id, order->asset, order->id, MARKET_ROLE_EXPIRE, atof(str_leave));
            if (res < 0) {
                log_fatal("append_user_envelope_history fail: %d, envelope_id: %"PRIu64"", res, order->id);
                return res;
            }
            order_finish(true, m, order);
        }
        return 14;
    }
    */

    // The envelope can only be opened once by the same person
    json_t *json_uids = json_loads(order->uids, 0, NULL);
    for (size_t i = 0; i < json_array_size(json_uids); ++i)
    {
        uint32_t opened_uid = json_integer_value(json_array_get(json_uids, i));
        if (opened_uid == user_id) {
            if (real) {
                *result = get_order_info(order, i);
            }
            return 0;
        }
    }

    uint32_t count = order->count + 1;
    json_t *json_amounts = json_loads(order->amounts, 0, NULL);
    char *balance = json_string_value(json_array_get(json_amounts, order->count));
    double leave = order->leave - atof(balance);

    int ret = 0;
    if (real) {
        if (order->user_id == user_id) {
            ret = balance_unfreeze_req(order->user_id, order->asset, balance, order->id, BALANCE_ACTION_SELF);
            if (ret != 0) {
                log_error("balance unfreeze request error");
                return ret;
            }
        } else {
            ret = balance_withdraw_req(order->user_id, order->asset, balance, order->id);
            if (ret != 0) {
                log_error("balance withdraw request error");
                return ret;
            }

            ret = balance_update_req(user_id, order->asset, balance, order->id);
            if (ret != 0) {
                log_error("balance update request error");
                return ret;
            }
        }
    }

    order->count = count;
    order->leave = leave;

    json_t *uids = json_array();
    for (size_t i = 0; i < json_array_size(json_uids); ++i)
    {
        json_array_append_new(uids, json_array_get(json_uids, i));
    }
    json_array_append_new(uids, json_integer(user_id));
    order->uids = json_dumps(uids, 0);

    json_t *json_times = json_loads(order->times, 0, NULL);
    json_t *times = json_array();
    for (size_t i = 0; i < json_array_size(json_uids); ++i)
    {
        json_array_append_new(times, json_array_get(json_times, i));
    }
    json_array_append_new(times, json_real(current_time));
    order->times = json_dumps(times, 0);

    if (real) {
        ret = append_user_envelope_history(current_time, user_id, order->asset, order->id, MARKET_ROLE_TAKER, atof(balance));
        if (ret < 0) {
            log_fatal("append_user_envelope_history fail: %d, envelope_id: %"PRIu64"", ret, order->id);
            return ret;
        }
    }

    if (real) {
        *result = get_order_info(order, count - 1);
        if (count == order->share) {
            order_finish(real, m, order);
        }
    } else {
        if (count == order->share) {
            order_finish(real, m, order);
        }
    }

    return 0;
}


