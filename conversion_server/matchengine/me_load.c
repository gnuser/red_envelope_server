/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/04, create
 */

# include "ut_mysql.h"
# include "me_trade.h"
# include "me_market.h"
# include "me_update.h"
# include "me_balance.h"

int load_orders(MYSQL *conn, const char *table)
{
    size_t query_limit = 1000;
    uint64_t last_id = 0;
    while (true) {
        sds sql = sdsempty();
        sql = sdscatprintf(sql, "SELECT `id`, `t`, `side`, `create_time`, `update_time`, `user_id`, `market`, `price`, "
            "`amount`, `taker_fee`, `maker_fee`, `left`, `freeze`, `deal_stock`, `deal_money`, `deal_fee`, `token`, "
            "`token_rate`, `asset_rate`, `discount`, `deal_token` FROM `%s` "
            "WHERE `id` > %"PRIu64" ORDER BY `id` LIMIT %zu", table, last_id, query_limit);

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
            last_id = strtoull(row[0], NULL, 0);
            market_t *market = get_market(row[6]);
            if (market == NULL)
                continue;

            order_t *order = malloc(sizeof(order_t));
            memset(order, 0, sizeof(order_t));
            order->id = strtoull(row[0], NULL, 0);
            order->type = strtoul(row[1], NULL, 0);
            order->side = strtoul(row[2], NULL, 0);
            order->create_time = strtod(row[3], NULL);
            order->update_time = strtod(row[4], NULL);
            order->user_id = strtoul(row[5], NULL, 0);
            order->market = strdup(row[6]);
            order->price = decimal(row[7], market->money_prec);
            order->amount = decimal(row[8], market->stock_prec);
            order->taker_fee = decimal(row[9], market->fee_prec);
            order->maker_fee = decimal(row[10], market->fee_prec);
            order->left = decimal(row[11], market->stock_prec);
            order->freeze = decimal(row[12], 0);
            order->deal_stock = decimal(row[13], 0);
            order->deal_money = decimal(row[14], 0);
            order->deal_fee = decimal(row[15], 0);
            order->token = strdup(row[16]);
            order->token_rate = decimal(row[17], 0);
            order->asset_rate = decimal(row[18], 0);
            order->discount = decimal(row[19], 0);
            order->deal_token = decimal(row[20], 0);

            if (!order->market || !order->price || !order->amount || !order->taker_fee || !order->maker_fee || !order->left ||
                    !order->freeze || !order->deal_stock || !order->deal_money || !order->deal_fee) {
                log_error("get order detail of order id: %"PRIu64" fail", order->id);
                mysql_free_result(result);
                return -__LINE__;
            }

            market_put_order(market, order);
        }
        mysql_free_result(result);

        if (num_rows < query_limit)
            break;
    }

    return 0;
}

int load_balance(MYSQL *conn, const char *table)
{
    size_t query_limit = 1000;
    uint64_t last_id = 0;
    while (true) {
        sds sql = sdsempty();
        sql = sdscatprintf(sql, "SELECT `id`, `user_id`, `asset`, `t`, `balance` FROM `%s` "
                "WHERE `id` > %"PRIu64" ORDER BY id LIMIT %zu", table, last_id, query_limit);
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
            last_id = strtoull(row[0], NULL, 0);
            uint32_t user_id = strtoul(row[1], NULL, 0);
            const char *asset = row[2];
            if (!asset_exist(asset)) {
                continue;
            }
            uint32_t type = strtoul(row[3], NULL, 0);
            mpd_t *balance = decimal(row[4], asset_prec(asset));
            balance_set(user_id, type, asset, balance);
        }
        mysql_free_result(result);

        if (num_rows < query_limit)
            break;
    }

    return 0;
}

static int load_update_balance(json_t *params)
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
    int prec = asset_prec(asset);
    if (prec < 0)
        return 0;

    // business
    if (!json_is_string(json_array_get(params, 2)))
        return -__LINE__;
    const char *business = json_string_value(json_array_get(params, 2));

    // business_id
    if (!json_is_integer(json_array_get(params, 3)))
        return -__LINE__;
    uint64_t business_id = json_integer_value(json_array_get(params, 3));

    // change
    if (!json_is_string(json_array_get(params, 4)))
        return -__LINE__;
    mpd_t *change = decimal(json_string_value(json_array_get(params, 4)), prec);
    if (change == NULL)
        return -__LINE__;

    // detail
    json_t *detail = json_array_get(params, 5);
    if (!json_is_object(detail)) {
        mpd_del(change);
        return -__LINE__;
    }

    int ret = update_user_balance(false, user_id, asset, business, business_id, change, detail);
    mpd_del(change);

    if (ret < 0) {
        return -__LINE__;
    }

    return 0;
}

static int load_freeze_balance(json_t *params)
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
    int prec = asset_prec(asset);
    if (prec < 0)
        return 0;

    // business
    if (!json_is_string(json_array_get(params, 2)))
        return -__LINE__;
    char *business = json_string_value(json_array_get(params, 2));
  
    // business_id
    if (!json_is_integer(json_array_get(params, 3)))
        return -__LINE__;
    uint64_t business_id = json_integer_value(json_array_get(params, 3));

    // change
    if (!json_is_string(json_array_get(params, 4)))
        return -__LINE__;
    mpd_t *change = decimal(json_string_value(json_array_get(params, 4)), prec);
    if (change == NULL)
        return -__LINE__;

    // detail
    json_t *detail = json_array_get(params, 5);
    if (!json_is_object(detail)) {
        mpd_del(change);
        return -__LINE__;
    }
	int ret = pledge_user_balance(false,user_id,asset,"pledge",business_id,change,detail);

    mpd_del(change);

    if (ret < 0) {
        return -__LINE__;
    }

    return 0;
}


// token discount
static int load_limit_order(json_t *params)
{
    if (json_array_size(params) != 10)
        return -__LINE__;

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return -__LINE__;
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return 0;

    // side
    if (!json_is_integer(json_array_get(params, 2)))
        return -__LINE__;
    uint32_t side = json_integer_value(json_array_get(params, 2));
    if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return -__LINE__;

    mpd_t *amount = NULL;
    mpd_t *price  = NULL;
    mpd_t *taker_fee = NULL;
    mpd_t *maker_fee = NULL;

    // amount
    if (!json_is_string(json_array_get(params, 3)))
        goto error;
    amount = decimal(json_string_value(json_array_get(params, 3)), market->stock_prec);
    if (amount == NULL)
        goto error;
    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) <= 0)
        goto error;

    // price 
    if (!json_is_string(json_array_get(params, 4)))
        goto error;
    price = decimal(json_string_value(json_array_get(params, 4)), market->money_prec);
    if (price == NULL) 
        goto error;
    if (mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0)
        goto error;

    // taker fee
    if (!json_is_string(json_array_get(params, 5)))
        goto error;
    taker_fee = decimal(json_string_value(json_array_get(params, 5)), market->fee_prec);
    if (taker_fee == NULL)
        goto error;
    if (mpd_cmp(taker_fee, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(taker_fee, mpd_one, &mpd_ctx) >= 0)
        goto error;

    // maker fee
    if (!json_is_string(json_array_get(params, 6)))
        goto error;
    maker_fee = decimal(json_string_value(json_array_get(params, 6)), market->fee_prec);
    if (maker_fee == NULL)
        goto error;
    if (mpd_cmp(maker_fee, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(maker_fee, mpd_one, &mpd_ctx) >= 0)
        goto error;

    // source
    if (!json_is_string(json_array_get(params, 7)))
        goto error;
    const char *source = json_string_value(json_array_get(params, 7));
    if (strlen(source) > SOURCE_MAX_LEN)
        goto error;

    // token
    if (!json_is_string(json_array_get(params, 8)))
        goto error;
    const char *token = json_string_value(json_array_get(params, 8));

    // discount
    mpd_t *discount = mpd_new(&mpd_ctx);
    mpd_t *token_rate = mpd_new(&mpd_ctx);
    mpd_t *asset_rate = mpd_new(&mpd_ctx);

    if (strlen(token) == 0) {
        mpd_copy(discount, mpd_zero, &mpd_ctx);
        mpd_copy(token_rate, mpd_zero, &mpd_ctx);
        mpd_copy(asset_rate, mpd_zero, &mpd_ctx);
    } else {
        if (asset_exist(token)) {
            if (!json_is_string(json_array_get(params, 9)))
                goto error;
            discount = decimal(json_string_value(json_array_get(params, 9)), 0);
            if (discount == NULL || mpd_cmp(discount, mpd_zero, &mpd_ctx) <= 0)
                goto error;    
        } else {
            mpd_del(discount);
            goto error; 
        }

        const char *cny = "CNY";
        sds token_cny = sdsempty();
        sdscpy(token_cny, token); 
        token_cny = sdscat(token_cny, cny);

        if (get_market(token_cny) == NULL)
            goto error;

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
            asset_cny = sdscat(asset_cny, "CNY");
            if (get_market(asset_cny) == NULL)
                goto error;

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

    int ret = market_put_limit_order(false, NULL, market, user_id, side, amount, price, taker_fee, maker_fee, source, 
                                     token, discount, token_rate, asset_rate);
    mpd_del(amount);
    mpd_del(price);
    mpd_del(taker_fee);
    mpd_del(maker_fee);
    mpd_del(discount);
    mpd_del(token_rate);
    mpd_del(asset_rate);

    return ret;

error:
    if (amount)
        mpd_del(amount);
    if (price)
        mpd_del(price);
    if (taker_fee)
        mpd_del(taker_fee);
    if (maker_fee)
        mpd_del(maker_fee);

    return -__LINE__;
}


// token discount
static int load_market_order(json_t *params)
{
    if (json_array_size(params) != 8)
        return -__LINE__;

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return -__LINE__;
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return 0;

    // side
    if (!json_is_integer(json_array_get(params, 2)))
        return -__LINE__;
    uint32_t side = json_integer_value(json_array_get(params, 2));
    if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return -__LINE__;

    mpd_t *amount = NULL;
    mpd_t *taker_fee = NULL;

    // amount
    if (!json_is_string(json_array_get(params, 3)))
        goto error;
    amount = decimal(json_string_value(json_array_get(params, 3)), market->stock_prec);
    if (amount == NULL)
        goto error;
    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) <= 0)
        goto error;

    // taker fee
    if (!json_is_string(json_array_get(params, 4)))
        goto error;
    taker_fee = decimal(json_string_value(json_array_get(params, 4)), market->fee_prec);
    if (taker_fee == NULL)
        goto error;
    if (mpd_cmp(taker_fee, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(taker_fee, mpd_one, &mpd_ctx) >= 0)
        goto error;

    // source
    if (!json_is_string(json_array_get(params, 5)))
        goto error;
    const char *source = json_string_value(json_array_get(params, 5));
    if (strlen(source) > SOURCE_MAX_LEN)
        goto error;

    // token
    if (!json_is_string(json_array_get(params, 6)))
        goto error;
    const char *token = json_string_value(json_array_get(params, 6));

    // discount
    mpd_t *discount = mpd_new(&mpd_ctx);
    mpd_t *token_rate = mpd_new(&mpd_ctx);
    mpd_t *asset_rate = mpd_new(&mpd_ctx);

    if (strlen(token) == 0) {
        mpd_copy(discount, mpd_zero, &mpd_ctx);
        mpd_copy(token_rate, mpd_zero, &mpd_ctx);
        mpd_copy(asset_rate, mpd_zero, &mpd_ctx);
    } else {
        if (asset_exist(token)) {
            if (!json_is_string(json_array_get(params, 7)))
                goto error;
            discount = decimal(json_string_value(json_array_get(params, 7)), 0);
            if (discount == NULL || mpd_cmp(discount, mpd_zero, &mpd_ctx) <= 0)
                goto error;     
        } else {
            mpd_del(discount);
            goto error; 
        }

        const char *cny = "CNY";
        sds token_cny = sdsempty();
        sdscpy(token_cny, token); 
        token_cny = sdscat(token_cny, cny);

        if (get_market(token_cny) == NULL)
            goto error;

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
            asset_cny = sdscat(asset_cny, "CNY");
            if (get_market(asset_cny) == NULL)
                goto error;

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

    int ret = market_put_market_order(false, NULL, market, user_id, side, amount, taker_fee, source, 
                                      token, discount, token_rate, asset_rate);

    mpd_del(amount);
    mpd_del(taker_fee);
    mpd_del(discount);
    mpd_del(token_rate);
    mpd_del(asset_rate);

    return ret;

error:
    if (amount)
        mpd_del(amount);
    if (taker_fee)
        mpd_del(taker_fee);

    return -__LINE__;
}

static int load_cancel_order(json_t *params)
{
    if (json_array_size(params) != 3)
        return -__LINE__;

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return -__LINE__;
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return 0;

    // order_id
    if (!json_is_integer(json_array_get(params, 2)))
        return -__LINE__;
    uint64_t order_id = json_integer_value(json_array_get(params, 2));

    order_t *order = market_get_order(market, order_id);
    if (order == NULL) {
        return -__LINE__;
    }

    int ret = market_cancel_order(false, NULL, market, order);
    if (ret < 0) {
        log_error("market_cancel_order id: %"PRIu64", user id: %u, market: %s", order_id, user_id, market_name);
        return -__LINE__;
    }

    return 0;
}

//#ifdef CONVERSION
// token discount
static int load_conversion(json_t *params)
{
    //put_conversion(role,user_id,stock,money,volume,price) --> maker调用形式
    //put_conversion(role,user_id,stock,money,volume,conversion_id) --->toker调用形式
    if (json_array_size(params) != 6 )
        return -__LINE__;

    // role
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint32_t role = json_integer_value(json_array_get(params, 0));

    // user_id
    if (!json_is_integer(json_array_get(params, 1)))
        return -__LINE__;
    uint32_t user_id = json_integer_value(json_array_get(params, 1));

    //stock name
    if (!json_is_string(json_array_get(params, 2)))
        return -__LINE__;
    const char *stock_name = json_string_value(json_array_get(params, 2));

    //money name
    if (!json_is_string(json_array_get(params, 3)))
        return -__LINE__;
    const char *money_name = json_string_value(json_array_get(params, 3));

    //check market
    sds market_name = sdsempty();
    sdscpy(market_name,stock_name);
    market_name = sdscat(market_name,money_name);
    market_t *market = get_market(market_name);
    if (market == NULL)
        return  -__LINE__;

    //volume
    if (!json_is_string(json_array_get(params, 4)))
        return  -__LINE__;
    const char *volume = json_string_value(json_array_get(params, 4));


    if(1 == role)
    {
        //price
        if (!json_is_string(json_array_get(params, 5)))
            return -__LINE__;

        mpd_t* amount    = decimal(volume, market->stock_prec);
        mpd_t* price     = decimal(json_string_value(json_array_get(params, 5)), market->money_prec);

        json_t *ret_result = NULL;
        int ret =  market_put_conversion_maker(false,ret_result,market,user_id,amount,price);
        mpd_del(amount);
        mpd_del(price);

        return ret;
    }
    else if(2 == role)//put_conversion(role,user_id,stock,money,volume,conversion_id)	  --> taker调用形式
    {
        // conversion_id
        if (!json_is_integer(json_array_get(params, 1)))
            return -__LINE__;
        uint64_t conversion_id  = json_integer_value(json_array_get(params, 5));
        mpd_t *amount = decimal(volume, market->stock_prec);
        order_t *order = market_get_order(market, conversion_id);
        if (order == NULL)
        {
            return  -__LINE__;
        }

        json_t *ret_result = NULL;
        int ret = market_put_conversion_taker(true, &ret_result, market, user_id, order, amount, stock_name, money_name);
        mpd_del(amount);
        json_decref(ret_result);
        return ret;
    }

    return -__LINE__;
}

//#endif

static int load_oper(json_t *detail)
{
    const char *method = json_string_value(json_object_get(detail, "method"));
    if (method == NULL)
        return -__LINE__;
    json_t *params = json_object_get(detail, "params");
    if (params == NULL || !json_is_array(params))
        return -__LINE__;


    int ret = 0;
    if (strcmp(method, "update_balance") == 0) {
        ret = load_update_balance(params);

    }
#ifdef FREEZE_BALANCE 
	else if (strcmp(method, "freeze_balance") == 0) {
        ret = load_freeze_balance(params);
        log_stderr("load_oper load_freeze_balance ret: %d", ret);
    }
#endif
    else if (strcmp(method, "limit_order") == 0) {
        ret = load_limit_order(params);
    } else if (strcmp(method, "market_order") == 0) {
        ret = load_market_order(params);
    } else if (strcmp(method, "cancel_order") == 0) {
        ret = load_cancel_order(params);
    } else if (strcmp(method, "put_conversion") == 0){
        ret = load_conversion(params);
    }
     else {
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
        sql = sdscatprintf(sql, "SELECT `id`, `detail` from `%s` WHERE `id` > %"PRIu64" ORDER BY `id` LIMIT %zu", table, last_id, query_limit);
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
            last_id = id;
            json_t *detail = json_loadb(row[1], strlen(row[1]), 0, NULL);
            if (detail == NULL) {
                log_error("invalid detail data: %s", row[1]);
                mysql_free_result(result);
                return -__LINE__;
            }
            ret = load_oper(detail);
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

