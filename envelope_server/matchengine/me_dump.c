/*
 * Description:
 *     History: yang@haipo.me, 2017/04/04, create
 */

# include <curl/curl.h>

# include "ut_mysql.h"
# include "me_trade.h"
# include "me_market.h"
# include "me_balance.h"

static sds sql_append_mpd(sds sql, mpd_t *val, bool comma)
{
    char *str = mpd_to_sci(val, 0);
    sql = sdscatprintf(sql, "'%s'", str);
    if (comma) {
        sql = sdscatprintf(sql, ", ");
    }
    free(str);
    return sql;
}

static int dump_orders_list(MYSQL *conn, const char *table, market_t *m, skiplist_t *list)
{
    sds sql = sdsempty();

    size_t insert_limit = 1000;
    size_t index = 0;
    skiplist_iter *iter = skiplist_get_iterator(list);
    skiplist_node *node;
    while ((node = skiplist_next(iter)) != NULL) {
        order_t *order = node->value;

        if (order->user_id < 1 || order->count >= order->share || order->share > MAX_ENVELOPE_SHARE)
            continue;

        if (index == 0) {
            sql = sdscatprintf(sql, "INSERT INTO `%s` (`time`, `envelope_id`, `user_id`, `asset`, `type`, `supply`, "
                    "`leave`, `share`, `expire_time`, `count`, `history`) VALUES ", table);
        } else {
            sql = sdscatprintf(sql, ", ");
        }

        char str_supply[24] = {0};
        sprintf(str_supply, "%.8f", order->supply);
        char str_leave[24] = {0};
        sprintf(str_leave, "%.8f", order->leave);

        // envelope
        json_t *history = json_array();
        for (int i = 0; i < order->count; ++i) {
            json_t *item = json_object();
            json_t *uids = json_loads(order->uids, 0, NULL);
            json_t *amounts = json_loads(order->amounts, 0, NULL);
            json_t *times = json_loads(order->times, 0, NULL);
            json_object_set_new(item, "u", json_array_get(uids, i));
            json_object_set_new(item, "a", json_array_get(amounts, i));
            json_object_set_new(item, "t", json_array_get(times, i));
            json_array_append_new(history, item);
        }

        sql = sdscatprintf(sql, "(%f, %"PRIu64", %u, '%s', %d, '%s', '%s', %d, %d, %d, '%s')", order->create_time, order->id, order->user_id,
               order->asset, order->type, str_supply, str_leave, order->share, order->expire_time, order->count, json_dumps(history, 0));

        index += 1;
        if (index == insert_limit) {
            log_trace("exec sql: %s", sql);
            int ret = mysql_real_query(conn, sql, sdslen(sql));
            if (ret < 0) {
                log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
                skiplist_release_iterator(iter);
                sdsfree(sql);
                return -__LINE__;
            }
            sdsclear(sql);
            index = 0;
        }
    }
    skiplist_release_iterator(iter);

    if (index > 0) {
        log_trace("exec sql: %s", sql);
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret < 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            return -__LINE__;
        }
    }

    sdsfree(sql);
    return 0;
}


int dump_orders(MYSQL *conn, const char *table)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "DROP TABLE IF EXISTS `%s`", table);
    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsclear(sql);

    sql = sdscatprintf(sql, "CREATE TABLE IF NOT EXISTS `%s` LIKE `slice_envelope_example`", table);
    log_trace("exec sql: %s", sql);
    ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsfree(sql);

    market_t *market = get_market(settings.markets[0].name);
    if (market == NULL) {
        return -__LINE__;
    }

    ret = dump_orders_list(conn, table, market, market->asks);
    if (ret < 0) {
        log_error("dump market: %s orders list fail: %d", market->name, ret);
        return -__LINE__;
    }

    return 0;
}



