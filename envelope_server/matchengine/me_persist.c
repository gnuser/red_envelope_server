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

static int load_slice_from_db(MYSQL *conn, time_t timestamp, market_t *market)
{
    sds table = sdsempty();

    table = sdscatprintf(table, "slice_envelope_%ld", timestamp);
    log_stderr("load envelope from: %s", table);

    int ret = load_orders(conn, table, market);
    if (ret < 0) {
        log_error("load_orders from %s fail: %d", table, ret);
        log_stderr("load_orders from %s fail: %d", table, ret);
        sdsfree(table);
        return -__LINE__;
    }

    sdsclear(table);
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

int init_from_db(market_t *market)
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
        ret = load_slice_from_db(conn, last_slice_time, market);
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
    log_stderr("init asset form db");
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

int init_market_from_db(MYSQL *conn)
{
    settings.market_num = 1;
    settings.markets = malloc(sizeof(struct market) * 1);
    for (size_t i = 0; i < settings.market_num; ++i) {
        settings.markets[i].min_amount = decimal("0.001", 0);
        settings.markets[i].name = "ETHBTC";
        settings.markets[i].money = "ETH";
        settings.markets[i].stock = "BTC";
        settings.markets[i].fee_prec = 4;
        settings.markets[i].stock_prec = 8;
        settings.markets[i].money_prec = 8;
    }

    return 0;
}

int asset_update(const char *asset)
{
    MYSQL *conn = mysql_connect(&settings.db_history);
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
    MYSQL *conn = mysql_connect(&settings.db_history);
    if (conn == NULL) {
        log_error("connect mysql fail");
        log_stderr("connect mysql fail");
        return -__LINE__;
    }

    init_asset_from_db(conn);
    if (market)
        init_market_from_db(conn);
    mysql_close(conn);
    return 0;
}

static int dump_order_to_db(MYSQL *conn, time_t end)
{
    sds table = sdsempty();
    table = sdscatprintf(table, "slice_envelope_%ld", end);
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
    sql = sdscatprintf(sql, "DROP TABLE `slice_envlope_%ld`", timestamp);
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


int get_envelope_from_db(json_t *envelope, uint64_t envelope_id)
{
    MYSQL *conn = mysql_connect(&settings.db_log);
    if (conn == NULL) {
        log_error("connect mysql fail");
        log_stderr("connect mysql fail");
        return __LINE__;
    }

    sds sql = sdsempty();
    sql = sdscatprintf(sql, "SELECT time, envelope_id, user_id, asset, type, supply, share, expire_time "
                       "from envelope_detail where envelope_id=%"PRIu64"", envelope_id);
    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        log_stderr("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return __LINE__;
    }
    sdsfree(sql);

    MYSQL_RES *result = mysql_store_result(conn);
    size_t num_rows = mysql_num_rows(result);

    if (num_rows == 0)
        return 1;

    MYSQL_ROW row = mysql_fetch_row(result);

    double time = strtod(row[0], NULL);
    json_object_set_new(envelope, "time", json_real(time));
    json_object_set_new(envelope, "envelope_id", json_integer(envelope_id));
    uint32_t user_id = strtoul(row[2], NULL, 0);
    json_object_set_new(envelope, "user_id", json_integer(user_id));
    json_object_set_new(envelope, "asset", json_string(row[3]));
    uint32_t type = strtoul(row[4], NULL, 0);
    json_object_set_new(envelope, "type", json_integer(type));
    json_object_set_new(envelope, "supply", json_string(row[5]));
    uint32_t share = strtoul(row[6], NULL, 0);
    json_object_set_new(envelope, "share", json_integer(share));
    uint32_t expire_time = strtoul(row[7], NULL, 0);
    json_object_set_new(envelope, "expire_time", json_integer(expire_time));

    mysql_free_result(result);
    mysql_close(conn);

    MYSQL *conn_history = mysql_connect(&settings.db_log);
    if (conn_history == NULL) {
        log_error("connect mysql fail");
        log_stderr("connect mysql fail");
        return __LINE__;
    }

    sds sql_history = sdsempty();
    sql_history = sdscatprintf(sql_history, "SELECT time, user_id, amount from user_envelope_history WHERE"
                  " envelope_id=%"PRIu64" AND role=2", envelope_id);
    log_trace("exec sql: %s", sql_history);

    ret = mysql_real_query(conn_history, sql_history, sdslen(sql_history));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql_history, mysql_errno(conn_history), mysql_error(conn_history));
        log_stderr("exec sql: %s fail: %d %s", sql_history, mysql_errno(conn_history), mysql_error(conn_history));
        sdsfree(sql_history);
        return __LINE__;
    }
    sdsfree(sql_history);

    MYSQL_RES *result_history = mysql_store_result(conn_history);
    size_t rows = mysql_num_rows(result_history);
    json_t *history = json_array();

    for (size_t i = 0; i < rows; ++i) {
        json_t *item = json_object();
        MYSQL_ROW row = mysql_fetch_row(result_history);
        double time = strtod(row[0], NULL);
        json_object_set_new(item, "time", json_real(time));
        uint32_t user_id = strtoul(row[1], NULL, 0);
        json_object_set_new(item, "uid", json_integer(user_id));
        json_object_set_new(item, "amount", json_string(row[2]));
        json_array_append_new(history, item);
    }
    json_object_set_new(envelope, "history", history);

    mysql_free_result(result_history);
    mysql_close(conn_history);
    return 0;
}

int get_user_envelope_history(json_t *res, uint32_t user_id, const char *asset, uint32_t start_time, uint32_t end_time,
                                  size_t offset, size_t limit, int role)
{
    MYSQL *conn = mysql_connect(&settings.db_log);
    if (conn == NULL) {
        log_error("connect mysql fail");
        log_stderr("connect mysql fail");
        return -__LINE__;
    }

    sds sql = sdsempty();
    sql = sdscatprintf(sql, "SELECT `time`, `user_id`, `asset`, `envelope_id`, `role`, `amount` from "
                       "`user_envelope_history` WHERE `user_id` = %u", user_id);

    size_t asset_len = strlen(asset);
    if (asset_len) {
        char _asset[2 * asset_len + 1];
        mysql_real_escape_string(conn, _asset, asset, asset_len);
        sql = sdscatprintf(sql, " AND `asset` = '%s'", _asset);
    }
    if (role) {
        sql = sdscatprintf(sql, " AND `role` = %d", role);
    }
    if (start_time) {
        sql = sdscatprintf(sql, " AND `time` >= %"PRIu64, start_time);
    }
    if (end_time) {
        sql = sdscatprintf(sql, " AND `time` < %"PRIu64, end_time);
    }

    sql = sdscatprintf(sql, " ORDER BY `id` DESC");
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
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        log_stderr("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsfree(sql);

    MYSQL_RES *result = mysql_store_result(conn);
    size_t num_rows = mysql_num_rows(result);
    json_t *records = json_array();
    for (size_t i = 0; i < num_rows; ++i) {
        json_t *record = json_object();
        MYSQL_ROW row = mysql_fetch_row(result);

        double time = strtod(row[0], NULL);
        json_object_set_new(record, "time", json_real(time));
        uint32_t user_id = strtoul(row[1], NULL, 0);
        json_object_set_new(record, "user", json_integer(user_id));
        json_object_set_new(record, "asset", json_string(row[2]));
        uint64_t envelope_id = strtoull(row[3], NULL, 0);
        json_object_set_new(record, "envelope_id", json_integer(envelope_id));
        uint32_t role = atoi(row[4]);
        json_object_set_new(record, "role", json_integer(role));
        json_object_set_new(record, "amount", json_string(row[5]));
        json_array_append_new(records, record);
    }

    json_object_set_new(res, "records", records);
    mysql_free_result(result);
    mysql_close(conn);
    return 0;
}


uint64_t get_user_envelope_history_total(uint32_t user_id, const char *asset, uint32_t start_time, uint32_t end_time, int role)
{
    MYSQL *conn = mysql_connect(&settings.db_log);
    if (conn == NULL) {
        log_error("connect mysql fail");
        log_stderr("connect mysql fail");
        return NULL;
    }

    sds sql = sdsempty();
    sql = sdscatprintf(sql, "SELECT count(*) FROM `user_envelope_history` WHERE `user_id` = %u", user_id);

    size_t asset_len = strlen(asset);
    if (asset_len) {
        char _asset[2 * asset_len + 1];
        mysql_real_escape_string(conn, _asset, asset, asset_len);
        sql = sdscatprintf(sql, " AND `asset` = '%s'", _asset);
    }
    if (role) {
        sql = sdscatprintf(sql, " AND `role` = %d", role);
    }
    if (start_time) {
        sql = sdscatprintf(sql, " AND `time` >= %"PRIu64, start_time);
    }
    if (end_time) {
        sql = sdscatprintf(sql, " AND `time` < %"PRIu64, end_time);
    }

    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_fatal("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return 0;
    }
    sdsfree(sql);

    MYSQL_RES *result = mysql_store_result(conn);
    MYSQL_ROW row = mysql_fetch_row(result);
    uint64_t total = strtoull(row[0], NULL, 0);
    mysql_free_result(result);
    return total;
}


// lalalala
int query_envelope_book(json_t *res, size_t book)
{
    MYSQL *conn = mysql_connect(&settings.db_log);
    if (conn == NULL) {
        log_error("connect mysql fail");
        log_stderr("connect mysql fail");
        return -__LINE__;
    }

    double end_time = current_timestamp(); 
    double start_time = end_time - 48.0 * 3600;
    if (book)
    {
        end_time -= 24.0 * 3600;
    }

    sds sql = sdsempty();
    
// SELECT envelope_id, SUM(if(`role`=1 and `time`>=1556086742.803295, CONVERT(amount, DECIMAL(20,8)), 0)) a, SUM(if(`role`!=1 and `time`>=1556086742.803295, CONVERT(amount, DECIMAL(20,8)), 0)) b from `user_envelope_history` group by `envelope_id`;

    sql = sdscatprintf(sql, "SELECT envelope_id, SUM(if(`role`=1 and `time`<=%f, CONVERT(amount, DECIMAL(20,8)), 0)) a, SUM(if(`role`!=1, CONVERT(amount, DECIMAL(20,8)), 0)) b"
          " from `user_envelope_history` where `time` >=%f group by `envelope_id`", end_time, start_time);

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
    json_t *records = json_array();
    for (size_t i = 0; i < num_rows; ++i) {
        json_t *record = json_object();
        MYSQL_ROW row = mysql_fetch_row(result);

        double id = strtoull(row[0], NULL, 0);
        double a = strtod(row[1], NULL);
        double b = strtod(row[2], NULL);
        if (a == b)
            continue;

        if (a == 0.0 && b > 0.0)
            continue;

        json_object_set_new(record, "id", json_integer(id));
        json_array_append_new(records, record);
    }

    json_object_set_new(res, "records", records);
    mysql_free_result(result);

    mysql_close(conn);
    return 0;
}


