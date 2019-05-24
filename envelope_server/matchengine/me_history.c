/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/06, create
 */

# include "me_config.h"
# include "me_history.h"
# include "me_balance.h"

static MYSQL *mysql_conn;
static nw_job *job;
static dict_t *dict_sql;
static nw_timer timer;

enum {
    USER_ENVELOPE_HISTORY,
    ENVELOPE_HISTORY,
    ENVELOPE_DETAIL
};

struct dict_sql_key {
    uint32_t type;
    uint32_t hash;
};

static uint32_t dict_sql_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct dict_sql_key));
}

static void *dict_sql_key_dup(const void *key)
{
    struct dict_sql_key *obj = malloc(sizeof(struct dict_sql_key));
    memcpy(obj, key, sizeof(struct dict_sql_key));
    return obj;
}

static int dict_sql_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct dict_sql_key));
}

static void dict_sql_key_free(void *key)
{
    free(key);
}

static void *on_job_init(void)
{
    return mysql_connect(&settings.db_log);
}

static void on_job(nw_job_entry *entry, void *privdata)
{
    MYSQL *conn = privdata;
    sds sql = entry->request;
    log_trace("exec sql: %s", sql);
    while (true) {
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0 && mysql_errno(conn) != 1062) {
            log_fatal("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            usleep(1000 * 1000);
            continue;
        }
        break;
    }
}

static void on_job_cleanup(nw_job_entry *entry)
{
    sdsfree(entry->request);
}

static void on_job_release(void *privdata)
{
    mysql_close(privdata);
}

static void on_timer(nw_timer *t, void *privdata)
{
    size_t count = 0;
    dict_iterator *iter = dict_get_iterator(dict_sql);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        nw_job_add(job, 0, entry->val);
        dict_delete(dict_sql, entry->key);
        count++;
    }
    dict_release_iterator(iter);

    if (count) {
        log_debug("flush history count: %zu", count);
    }
}

int init_history(void)
{
    mysql_conn = mysql_init(NULL);
    if (mysql_conn == NULL)
        return -__LINE__;
    if (mysql_options(mysql_conn, MYSQL_SET_CHARSET_NAME, settings.db_log.charset) != 0)
        return -__LINE__;

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = dict_sql_hash_function;
    dt.key_compare    = dict_sql_key_compare;
    dt.key_dup        = dict_sql_key_dup;
    dt.key_destructor = dict_sql_key_free;

    dict_sql = dict_create(&dt, 1024);
    if (dict_sql == 0) {
        return -__LINE__;
    }

    nw_job_type jt;
    memset(&jt, 0, sizeof(jt));
    jt.on_init    = on_job_init;
    jt.on_job     = on_job;
    jt.on_cleanup = on_job_cleanup;
    jt.on_release = on_job_release;

    job = nw_job_create(&jt, settings.history_thread);
    if (job == NULL)
        return -__LINE__;

    nw_timer_set(&timer, 0.1, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

int fini_history(void)
{
    on_timer(NULL, NULL);

    usleep(100 * 1000);
    nw_job_release(job);

    return 0;
}

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

static sds get_sql(struct dict_sql_key *key)
{
    dict_entry *entry = dict_find(dict_sql, key);
    if (!entry) {
        sds val = sdsempty();
        entry = dict_add(dict_sql, key, val);
        if (entry == NULL) {
            sdsfree(val);
            return NULL;
        }
    }
    return entry->val;
}

static void set_sql(struct dict_sql_key *key, sds sql)
{
    dict_entry *entry = dict_find(dict_sql, key);
    if (entry) {
        entry->val = sql;
    }
}

bool is_history_block(void)
{
    if (job->request_count >= MAX_PENDING_HISTORY) {
        return true;
    }
    return false;
}

sds history_status(sds reply)
{
    return sdscatprintf(reply, "history pending %d\n", job->request_count);
}

// envelope
int append_user_envelope_history(double time, uint32_t user_id, const char *asset, uint64_t envelope_id, uint32_t role, double amount)
{
    struct dict_sql_key key;
    key.hash = 1;
    key.type = USER_ENVELOPE_HISTORY;
    sds sql = get_sql(&key);
    if (sql == NULL)
        return -__LINE__;

    sql = sdscatprintf(sql, "INSERT INTO `user_envelope_history` (`time`, `user_id`, `asset`, `envelope_id`, "
                       "`role`, `amount`) VALUES ");
    char str_amount[24] = {0};
    sprintf(str_amount, "%.8f", amount);
    sql = sdscatprintf(sql, "(%f, %u, '%s', %"PRIu64", %d, '%s')", time, user_id, asset, envelope_id, role, str_amount);
    printf("sql: %s\n", sql);

    set_sql(&key, sql);
    return 0;
}


int append_envelope_detail(double time, uint64_t envelope_id, uint32_t user_id, const char *asset, uint32_t type, 
                           const char *supply, uint32_t share, uint32_t expire_time)
{
    struct dict_sql_key key;
    key.hash = 1;
    key.type = ENVELOPE_HISTORY;
    sds sql = get_sql(&key);
    if (sql == NULL)
        return -__LINE__;

    sql = sdscatprintf(sql, "INSERT INTO `envelope_detail` (`time`, `envelope_id`, `user_id`, `asset`,  "
                       "`type`, `supply`, `share`, `expire_time`) VALUES ");
    sql = sdscatprintf(sql, "(%f, %"PRIu64", %u, '%s', %d, '%s', %d, %d)", time, envelope_id, user_id, asset, 
                       type, supply, share, expire_time);

    set_sql(&key, sql);
    return 0;
}



