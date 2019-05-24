/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/04, create
 */

# include "me_config.h"
# include "me_persist.h"
# include "me_operlog.h"
# include "me_market.h"
# include "me_load.h"
# include "me_dump.h"

static time_t last_slice_time;
static nw_timer timer;

static time_t get_today_start(void)
{
    time_t now = time(NULL);
    struct tm *lt = localtime(&now);
    struct tm t;
    memset(&t, 0, sizeof(t));
    t.tm_year = lt->tm_year;
    t.tm_mon  = lt->tm_mon;
    t.tm_mday = lt->tm_mday;
    return mktime(&t);
}

static int get_last_slice(MYSQL *conn, time_t *timestamp, uint64_t *last_oper_id, uint64_t *last_order_id, uint64_t *last_deals_id)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "SELECT `time`, `end_oper_id`, `end_order_id`, `end_deals_id` from `slice_history` ORDER BY `id` DESC LIMIT 1");
    log_stderr("get last slice time");
    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        log_stderr("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsfree(sql);

    MYSQL_RES *result = mysql_store_result(conn);
    size_t num_rows = mysql_num_rows(result);
    if (num_rows != 1) {
        mysql_free_result(result);
        return 0;
    }

    MYSQL_ROW row = mysql_fetch_row(result);
    *timestamp = strtol(row[0], NULL, 0);
    *last_oper_id  = strtoull(row[1], NULL, 0);
    *last_order_id = strtoull(row[2], NULL, 0);
    *last_deals_id = strtoull(row[3], NULL, 0);
    mysql_free_result(result);

    return 0;
}

static int load_slice_from_db(MYSQL *conn, time_t timestamp)
{
    sds table = sdsempty();

    table = sdscatprintf(table, "slice_order_%ld", timestamp);
    log_stderr("load orders from: %s", table);
    int ret = load_orders(conn, table);
    if (ret < 0) {
        log_error("load_orders from %s fail: %d", table, ret);
        log_stderr("load_orders from %s fail: %d", table, ret);
        sdsfree(table);
        return -__LINE__;
    }

    sdsclear(table);
    table = sdscatprintf(table, "slice_balance_%ld", timestamp);
    log_stderr("load balance from: %s", table);
    ret = load_balance(conn, table);
    if (ret < 0) {
        log_error("load_balance from %s fail: %d", table, ret);
        log_stderr("load_balance from %s fail: %d", table, ret);
        sdsfree(table);
        return -__LINE__;
    }

    sdsfree(table);
    return 0;
}

static int load_operlog_from_db(MYSQL *conn, time_t date, uint64_t *start_id)
{
    struct tm *t = localtime(&date);
    sds table = sdsempty();
    table = sdscatprintf(table, "operlog_%04d%02d%02d", 1900 + t->tm_year, 1 + t->tm_mon, t->tm_mday);
    log_stderr("load oper log from: %s", table);
    if (!is_table_exists(conn, table)) {
        log_error("table %s not exist", table);
        log_stderr("table %s not exist", table);
        sdsfree(table);
        return 0;
    }

    int ret = load_operlog(conn, table, start_id);
    if (ret < 0) {
        log_error("load_operlog from %s fail: %d", table, ret);
        log_stderr("load_operlog from %s fail: %d", table, ret);
        sdsfree(table);
        return -__LINE__;
    }

    sdsfree(table);
    return 0;
}

int init_from_db(void)
{
    MYSQL *conn = mysql_connect(&settings.db_log);
    if (conn == NULL) {
        log_error("connect mysql fail");
        log_stderr("connect mysql fail");
        return -__LINE__;
    }

    time_t now = time(NULL);
    uint64_t last_oper_id  = 0;
    uint64_t last_order_id = 0;
    uint64_t last_deals_id = 0;
    int ret = get_last_slice(conn, &last_slice_time, &last_oper_id, &last_order_id, &last_deals_id);
    if (ret < 0) {
        return ret;
    }

    log_info("last_slice_time: %ld, last_oper_id: %"PRIu64", last_order_id: %"PRIu64", last_deals_id: %"PRIu64,
            last_slice_time, last_oper_id, last_order_id, last_deals_id);
    log_stderr("last_slice_time: %ld, last_oper_id: %"PRIu64", last_order_id: %"PRIu64", last_deals_id: %"PRIu64,
            last_slice_time, last_oper_id, last_order_id, last_deals_id);

    order_id_start = last_order_id;
    deals_id_start = last_deals_id;

    if (last_slice_time == 0) {
        ret = load_operlog_from_db(conn, now, &last_oper_id);
        if (ret < 0)
            goto cleanup;
    } else {
        ret = load_slice_from_db(conn, last_slice_time);
        if (ret < 0) {
            goto cleanup;
        }

        time_t begin = last_slice_time;
        time_t end = get_today_start() + 86400;
        while (begin < end) {
            ret = load_operlog_from_db(conn, begin, &last_oper_id);
            if (ret < 0) {
                goto cleanup;
            }
            begin += 86400;
        }
    }

    operlog_id_start = last_oper_id;

    mysql_close(conn);
    log_stderr("load success");

    return 0;

cleanup:
    mysql_close(conn);
    return ret;
}

int init_asset_from_db(MYSQL *conn)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "SELECT id, short_name from system_coin_type");
    log_stderr("init asset form bitasia db");
    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        log_stderr("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsfree(sql);
    MYSQL_RES *result = mysql_store_result(conn);
    size_t num_rows = mysql_num_rows(result);
    settings.asset_num = num_rows;

    settings.assets = malloc(sizeof(struct asset) * MAX_ASSET_NUM);
    for (size_t i = 0; i < settings.asset_num; ++i) {
        MYSQL_ROW row = mysql_fetch_row(result);
        int id = atoi(row[0]);
        const char *name = row[1];
        settings.assets[i].id = id;
        settings.assets[i].name = sdsnewlen(name, strlen(name)); 
        settings.assets[i].prec_save = 20;
        settings.assets[i].prec_show = 8;
        if (strlen(settings.assets[i].name) > ASSET_NAME_MAX_LEN)
            return -__LINE__;
    }
    
    mysql_free_result(result);
    return 0;
}

static int init_conversion_market()
{
    settings.market_num = settings.asset_num * (settings.asset_num - 1);
    settings.markets = malloc(sizeof(struct market) * settings.market_num);
    size_t k = 0;
    for (size_t i = 0; i < settings.asset_num; ++i)
    {
        for(size_t j = i+1; j < settings.asset_num; ++j)
        {
	    const char *min_amount = "0.00001";
            settings.markets[k].min_amount = decimal(min_amount, 0);
            settings.markets[k].money = sdsnewlen(settings.assets[i].name, strlen(settings.assets[i].name));
            settings.markets[k].stock = sdsnewlen(settings.assets[j].name, strlen(settings.assets[j].name));
            settings.markets[k].fee_prec = 4;
	    settings.markets[k].stock_prec = 9;
            settings.markets[k].money_prec = 9;
	    sds tmp = sdsempty();
	    sdscpy(tmp, settings.markets[k].stock);	
	    settings.markets[k].name = sdscat(tmp, settings.markets[k].money);

            k++;
            
	    settings.markets[k].min_amount = decimal(min_amount, 0);
            settings.markets[k].money = sdsnewlen(settings.assets[j].name, strlen(settings.assets[j].name));
            settings.markets[k].stock = sdsnewlen(settings.assets[i].name, strlen(settings.assets[i].name));
            settings.markets[k].fee_prec = 4;
            settings.markets[k].stock_prec = 9;
            settings.markets[k].money_prec = 9; 
	    sds tmp1 = sdsempty();
	    sdscpy(tmp1, settings.markets[k].stock);	
	    settings.markets[k].name = sdscat(tmp1, settings.markets[k].money);
	    k++;
        }
    }
    return 0;
}

int init_market_from_db(MYSQL *conn)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "SELECT buy_coin_id, sell_coin_id, min_count from system_trade_type where status=1");
    log_stderr("init market form bitasia db");
    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        log_stderr("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsfree(sql);

    MYSQL_RES *result = mysql_store_result(conn);
    size_t num_rows = mysql_num_rows(result);
    settings.market_num = num_rows;

    settings.markets = malloc(sizeof(struct market) * MAX_MARKET_NUM);
    for (size_t i = 0; i < settings.market_num; ++i) {
        MYSQL_ROW row = mysql_fetch_row(result);
        int buy_coin_id = atoi(row[0]);
        int sell_coin_id = atoi(row[1]);
        const char *min_amount = row[2];

        // if min_amount is 0, set min_amount = 0.0001
        mpd_t *tmp = NULL;
        tmp = decimal(min_amount, 0);
        if (tmp == NULL || mpd_cmp(tmp, mpd_zero, &mpd_ctx) <= 0)
            min_amount = "0.0001";

        if (tmp) 
            mpd_del(tmp);

        settings.markets[i].min_amount = decimal(min_amount, 0);
 
	    bool fbuy = false;
	    bool fsell = false;
        
        for (size_t j = 0; j < settings.asset_num; ++j) {
            if (!fbuy && buy_coin_id == settings.assets[j].id) {
                settings.markets[i].money = sdsnewlen(settings.assets[j].name, strlen(settings.assets[j].name)); 
                fbuy = true;
            }
            if (!fsell && sell_coin_id == settings.assets[j].id) {
                settings.markets[i].stock = sdsnewlen(settings.assets[j].name, strlen(settings.assets[j].name));
                fsell = true;
            }
            if (fbuy && fsell) {
		        sds tmp = sdsempty();
		        sdscpy(tmp, settings.markets[i].stock);	
                settings.markets[i].name = sdscat(tmp, settings.markets[i].money);
		        fbuy = false;
		        fsell = false;
                break;
            }
        }

        settings.markets[i].fee_prec = 4;
        settings.markets[i].stock_prec = 9;
        settings.markets[i].money_prec = 9; 
    }

    mysql_free_result(result);
    return 0;
}

int asset_update(const char *asset)
{
    MYSQL *conn = mysql_connect(&settings.db_bitasia);
    if (conn == NULL) {
        log_error("connect mysql fail");
        log_stderr("connect mysql fail");
        return -__LINE__;
    }

    sds sql = sdsempty();
    sql = sdscatprintf(sql, "INSERT INTO system_coin_type (short_name)VALUES(\"%s\")", asset);

    log_stderr("update asset");
    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        log_stderr("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsfree(sql);

    return 0; 
}

int init_asset_and_market(bool market)
{
    MYSQL *conn = mysql_connect(&settings.db_bitasia);
    if (conn == NULL) {
        log_error("connect mysql fail");
        log_stderr("connect mysql fail");
        return -__LINE__;
    }

    init_asset_from_db(conn);
    init_conversion_market();
    //if (market)
    //    init_market_from_db(conn);
    mysql_close(conn);

    return 0;
}

static int dump_order_to_db(MYSQL *conn, time_t end)
{
    sds table = sdsempty();
    table = sdscatprintf(table, "slice_order_%ld", end);
    log_info("dump order to: %s", table);
    int ret = dump_orders(conn, table);
    if (ret < 0) {
        log_error("dump_orders to %s fail: %d", table, ret);
        sdsfree(table);
        return -__LINE__;
    }
    sdsfree(table);

    return 0;
}

static int dump_balance_to_db(MYSQL *conn, time_t end)
{
    sds table = sdsempty();
    table = sdscatprintf(table, "slice_balance_%ld", end);
    log_info("dump balance to: %s", table);
    int ret = dump_balance(conn, table);
    if (ret < 0) {
        log_error("dump_balance to %s fail: %d", table, ret);
        sdsfree(table);
        return -__LINE__;
    }
    sdsfree(table);

    return 0;
}

int update_slice_history(MYSQL *conn, time_t end)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "INSERT INTO `slice_history` (`id`, `time`, `end_oper_id`, `end_order_id`, `end_deals_id`) VALUES (NULL, %ld, %"PRIu64", %"PRIu64", %"PRIu64")",
            end, operlog_id_start, order_id_start, deals_id_start);
    log_info("update slice history to: %ld", end);
    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret < 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsfree(sql);

    return 0;
}

int dump_to_db(time_t timestamp)
{
    MYSQL *conn = mysql_connect(&settings.db_log);
    if (conn == NULL) {
        log_error("connect mysql fail");
        return -__LINE__;
    }

    log_info("start dump slice, timestamp: %ld", timestamp);

    int ret;
    ret = dump_order_to_db(conn, timestamp);
    if (ret < 0) {
        goto cleanup;
    }

    ret = dump_balance_to_db(conn, timestamp);
    if (ret < 0) {
        goto cleanup;
    }

    ret = update_slice_history(conn, timestamp);
    if (ret < 0) {
        goto cleanup;
    }

    log_info("dump success");
    mysql_close(conn);
    return 0;

cleanup:
    log_info("dump fail");
    mysql_close(conn);
    return ret;
}

static int slice_count(MYSQL *conn, time_t timestamp)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "SELECT COUNT(*) FROM `slice_history` WHERE `time` >= %ld", timestamp - settings.slice_keeptime);
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
    if (num_rows != 1) {
        mysql_free_result(result);
        return -__LINE__;
    }

    MYSQL_ROW row = mysql_fetch_row(result);
    int count = atoi(row[0]);
    mysql_free_result(result);

    return count;
}

static int delete_slice(MYSQL *conn, uint64_t id, time_t timestamp)
{
    log_info("delete slice id: %"PRIu64", time: %ld start", id, timestamp);

    int ret;
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "DROP TABLE `slice_order_%ld`", timestamp);
    log_trace("exec sql: %s", sql);
    ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        return -__LINE__;
    }
    sdsclear(sql);

    sql = sdscatprintf(sql, "DROP TABLE `slice_balance_%ld`", timestamp);
    log_trace("exec sql: %s", sql);
    ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        return -__LINE__;
    }
    sdsclear(sql);

    sql = sdscatprintf(sql, "DELETE FROM `slice_history` WHERE `id` = %"PRIu64"", id);
    log_trace("exec sql: %s", sql);
    ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        return -__LINE__;
    }
    sdsfree(sql);

    log_info("delete slice id: %"PRIu64", time: %ld success", id, timestamp);

    return 0;
}

int clear_slice(time_t timestamp)
{
    MYSQL *conn = mysql_connect(&settings.db_log);
    if (conn == NULL) {
        log_error("connect mysql fail");
        return -__LINE__;
    }

    int ret = slice_count(conn, timestamp);
    if (ret < 0) {
        goto cleanup;
    }
    if (ret == 0) {
        log_error("0 slice in last %d seconds", settings.slice_keeptime);
        goto cleanup;
    }

    sds sql = sdsempty();
    sql = sdscatprintf(sql, "SELECT `id`, `time` FROM `slice_history` WHERE `time` < %ld", timestamp - settings.slice_keeptime);
    ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        ret = -__LINE__;
        goto cleanup;
    }
    sdsfree(sql);

    MYSQL_RES *result = mysql_store_result(conn);
    size_t num_rows = mysql_num_rows(result);
    for (size_t i = 0; i < num_rows; ++i) {
        MYSQL_ROW row = mysql_fetch_row(result);
        uint64_t id = strtoull(row[0], NULL, 0);
        time_t slice_time = strtol(row[1], NULL, 0);
        ret = delete_slice(conn, id, slice_time);
        if (ret < 0) {
            mysql_free_result(result);
            goto cleanup;
        }

    }
    mysql_free_result(result);

    mysql_close(conn);
    return 0;

cleanup:
    mysql_close(conn);
    return ret;
}

int make_slice(time_t timestamp)
{
    int pid = fork();
    if (pid < 0) {
        log_fatal("fork fail: %d", pid);
        return -__LINE__;
    } else if (pid > 0) {
        return 0;
    }

    int ret;
    ret = dump_to_db(timestamp);
    if (ret < 0) {
        log_fatal("dump_to_db fail: %d", ret);
    }

    ret = clear_slice(timestamp);
    if (ret < 0) {
        log_fatal("clear_slice fail: %d", ret);
    }

    exit(0);
    return 0;
}

static void on_timer(nw_timer *timer, void *privdata)
{
    time_t now = time(NULL);
    if ((now - last_slice_time) >= settings.slice_interval && (now % settings.slice_interval) <= 5) {
        make_slice(now);
        last_slice_time = now;
    }
}

int init_persist(void)
{
    nw_timer_set(&timer, 1.0, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}


mpd_t *get_user_freeze_balance(uint32_t user_id, const char *asset, int prec)
{
    MYSQL *conn = mysql_connect(&settings.db_history);
    if (conn == NULL) {
        log_error("connect mysql fail");
        return NULL;
    }

    sds sql = sdsempty();
    sql = sdscatprintf(sql, "SELECT `change` FROM `balance_history_%u` WHERE `user_id` = %u AND business='freeze'",
            user_id % HISTORY_HASH_NUM, user_id);

    size_t asset_len = strlen(asset);
    if (asset_len > 0) {
        char _asset[2 * asset_len + 1];
        mysql_real_escape_string(conn, _asset, asset, strlen(asset));
        sql = sdscatprintf(sql, " AND `asset` = '%s'", _asset);
    }

    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_fatal("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
		mysql_close(conn);
        return NULL;
    }
    sdsfree(sql);

    mpd_t *total = mpd_new(&mpd_ctx);
    mpd_set_string(total, "0", &mpd_ctx);

    MYSQL_RES *result = mysql_store_result(conn);
    size_t num_rows = mysql_num_rows(result);
    for (size_t i = 0; i < num_rows; ++i) {
        MYSQL_ROW row = mysql_fetch_row(result);
        mpd_t *change = decimal(row[0], prec);
        mpd_add(total, total, change, &mpd_ctx);
		mpd_del(change);
    }
    mysql_free_result(result);
	mysql_close(conn);
    return total;
}
