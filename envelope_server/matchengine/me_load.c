/*
 * Description:
 *     History: yang@haipo.me, 2017/04/04, create
 */

# include "ut_mysql.h"
# include "me_trade.h"
# include "me_market.h"
# include "me_update.h"
# include "me_balance.h"


// envelope
int load_orders(MYSQL *conn, const char *table, market_t *market)
{
    size_t query_limit = 1000;
    uint64_t last_id = 0;
    while (true) {
        sds sql = sdsempty();

        sql = sdscatprintf(sql, "SELECT `time`, `envelope_id`, `user_id`, `asset`, `type`, `supply`, `leave`, `share`, `expire_time`, "
            "`count`, `history` FROM `%s` WHERE `envelope_id` > %"PRIu64" ORDER BY `envelope_id` LIMIT %zu", table, last_id, query_limit);

        log_trace("exec sql: %s", sql);
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            return -__LINE__;
        }
        sdsfree(sql);

        MYSQL_RES *result = mysql_store_result(conn);
        size_t num_rows = mysql_num_rows(result);

        for (size_t i = 0; i < num_rows; ++i) {
            MYSQL_ROW row = mysql_fetch_row(result);

            last_id = strtoull(row[1], NULL, 0);

            order_t *order = malloc(sizeof(order_t));
            memset(order, 0, sizeof(order_t));

            order->side == MARKET_ORDER_SIDE_ASK;
            order->create_time = strtod(row[0], NULL);
            order->id = strtoull(row[1], NULL, 0);
            order->user_id = strtoul(row[2], NULL, 0);
            order->asset = strdup(row[3]);
            order->type = strtod(row[4], NULL);
            char *str_supply = row[5];
            order->supply = strtod(row[5], NULL);
            char *str_leave = row[6];
            order->leave = strtod(row[6], NULL);
            order->share = strtod(row[7], NULL);
            order->expire_time = strtod(row[8], NULL);
            order->count = strtod(row[9], NULL);
            json_t *json_history = json_loads(row[10], 0, NULL);

    // envelope
    json_t *json_uids = json_array();
    json_t *json_times = json_array();

    for (size_t i = 0; i < json_array_size(json_history); ++i)
    {
        json_t *item = json_array_get(json_history, i);
        json_array_append_new(json_uids, json_object_get(item, "u"));
        json_array_append_new(json_times, json_object_get(item, "t"));
    }
    order->uids = json_dumps(json_uids, 0);
    order->times = json_dumps(json_times, 0);

    double left = order->supply;
    double temp = 0;

    json_t *json_amounts = json_array();

    if (order->type == ENVELOPE_TYPE_AVERAGE) {
        temp = order->supply / order->share;

        char str_temp[24] = {0};
        sprintf(str_temp, "%.8f", temp);

        char str_share[24] = {0};
        sprintf(str_share, "%d", order->share - 1);

        mpd_t *supply_mp = NULL;
        mpd_t *temp_mp = NULL;
        mpd_t *share_mp = NULL;
        mpd_t *left_mp = mpd_new(&mpd_ctx);

        supply_mp = decimal(str_supply, 8);
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

        for (size_t i = 0; i < order->share; ++i)
        {
            if (i == 0) {
                json_array_append_new(json_amounts, json_string(str_left));
            } else {
                json_array_append_new(json_amounts, json_string(str_temp));
            }
        }
    } else {

        mpd_t *left_mp = NULL;
        left_mp = decimal(str_supply, 8);
        mpd_t *temp_mp = mpd_new(&mpd_ctx);

        for (size_t i = order->share; i > 1; --i)
        {
            srand((unsigned)(order->create_time));
            double deno = rand() / (double)(RAND_MAX / order->share) + 1.0;

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
    // envelope

            market_put_order(market, order);
        }
        mysql_free_result(result);

        if (num_rows < query_limit)
            break;
    }

    return 0;
}


// envelope
static int load_envelope_put(json_t *params, double time)
{
    if (json_array_size(params) != 6)
        return -__LINE__;

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // asset
    if (!json_is_string(json_array_get(params, 1)))
        return -__LINE__;
    const char *asset = json_string_value(json_array_get(params, 1));
    int prec = asset_prec_show(asset);
    if (prec < 0)
        return -__LINE__;

    // supply
    if (!json_is_string(json_array_get(params, 2)))
        return -__LINE__;
    const char *supply = json_string_value(json_array_get(params, 2));

    // share
    if (!json_is_integer(json_array_get(params, 3)))
        return -__LINE__;
    uint32_t share = json_integer_value(json_array_get(params, 3));
    if (share > MAX_ENVELOPE_SHARE)
        return -__LINE__;

    // type
    if (!json_is_integer(json_array_get(params, 4)))
        return -__LINE__;
    uint32_t type = json_integer_value(json_array_get(params, 4));
    if (type != ENVELOPE_TYPE_AVERAGE && type != ENVELOPE_TYPE_RANDOM)
        return -__LINE__;

    // expire_time
    uint32_t expire_time = json_integer_value(json_array_get(params, 5));
    if (expire_time == 0)
        expire_time = 24;

    json_t *result = NULL;
    market_t *market = get_market(settings.markets[0].name);
    if (market == NULL)
        return -__LINE__;
    int ret = envelope_put(false, &result, market, user_id, asset, supply, share, type, expire_time, time);
    return ret;
}


static int load_envelope_open(json_t *params)
{
    if (json_array_size(params) != 3)
        return -__LINE__;

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // asset
    if (!json_is_string(json_array_get(params, 1)))
        return -__LINE__;
    const char *asset = json_string_value(json_array_get(params, 1));
    int prec = asset_prec_show(asset);
    if (prec < 0)
        return -__LINE__;

    // order_id
    if (!json_is_integer(json_array_get(params, 2)))
        return -__LINE__;
    uint64_t order_id = json_integer_value(json_array_get(params, 2));

    market_t *market = get_market(settings.markets[0].name);
    if (market == NULL)
        return -__LINE__;

    order_t *order = market_get_order(market, order_id);
    if (order == NULL) {
        return -__LINE__;
    }

    if (strcmp(order->asset, asset) != 0) {
        return -__LINE__;
    }

    json_t *result = NULL;
    int ret = envelope_open(false, &result, market, user_id, order);
    return ret;
}

static int load_envelope_cancel(json_t *params)
{
    if (json_array_size(params) != 1)
        return -__LINE__;

    // order_id
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint64_t order_id = json_integer_value(json_array_get(params, 0));

    market_t *market = get_market(settings.markets[0].name);
    if (market == NULL)
        return -__LINE__;

    order_t *order = market_get_order(market, order_id);
    if (order == NULL) {
        return 0;
    }

    json_t *result = NULL;
    int ret = market_cancel_order(false, &result, market, order);
    if (ret < 0) {
        return -__LINE__;
    }

    json_decref(result);
    return 0;
}


static int load_oper(json_t *detail, double time)
{
    const char *method = json_string_value(json_object_get(detail, "method"));
    if (method == NULL)
        return -__LINE__;
    json_t *params = json_object_get(detail, "params");
    if (params == NULL || !json_is_array(params))
        return -__LINE__;

    int ret = 0;
    if (strcmp(method, "envelope_put") == 0) {
        ret = load_envelope_put(params, time);
    } else if (strcmp(method, "envelope_open") == 0) {
        ret = load_envelope_open(params);
    } else if (strcmp(method, "cancel_order") == 0) {
        ret = load_envelope_cancel(params);
    } else {
        return -__LINE__;
    }

    return ret;
}

int load_operlog(MYSQL *conn, const char *table, uint64_t *start_id)
{
    size_t query_limit = 1000;
    uint64_t last_id = *start_id;
    while (true) {
        sds sql = sdsempty();
        sql = sdscatprintf(sql, "SELECT `id`, `time`, `detail` from `%s` WHERE `id` > %"PRIu64" ORDER BY `id` LIMIT %zu", table, last_id, query_limit);
        log_trace("exec sql: %s", sql);
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            return -__LINE__;
        }
        sdsfree(sql);

        MYSQL_RES *result = mysql_store_result(conn);
        size_t num_rows = mysql_num_rows(result);
        for (size_t i = 0; i < num_rows; ++i) {
            MYSQL_ROW row = mysql_fetch_row(result);
            uint64_t id = strtoull(row[0], NULL, 0);
            if (id != last_id + 1) {
                log_error("invalid id: %"PRIu64", last id: %"PRIu64"", id, last_id);
                return -__LINE__;
            }

            double time = strtod(row[1], NULL);

            last_id = id;
            json_t *detail = json_loadb(row[2], strlen(row[2]), 0, NULL);
            if (detail == NULL) {
                log_error("invalid detail data: %s", row[2]);
                mysql_free_result(result);
                return -__LINE__;
            }
            ret = load_oper(detail, time);
            if (ret < 0) {
                json_decref(detail);
                log_error("load_oper: %"PRIu64":%s fail: %d", id, row[1], ret);
                mysql_free_result(result);
                return -__LINE__;
            }
            json_decref(detail);
        }
        mysql_free_result(result);
        if (num_rows < query_limit)
            break;
    }

    *start_id = last_id;
    return 0;
}

