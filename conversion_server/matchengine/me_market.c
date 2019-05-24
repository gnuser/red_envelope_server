/*
 * Description:
 *     History: yang@haipo.me, 2017/03/16, create
 */
# include <curl/curl.h>
# include "me_config.h"
# include "me_market.h"
# include "me_balance.h"
# include "me_history.h"
# include "me_message.h"


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
        cmp = mpd_cmp(order1->price, order2->price, &mpd_ctx);
    } else {
        cmp = mpd_cmp(order2->price, order1->price, &mpd_ctx);
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
    mpd_del(order->price);
    mpd_del(order->amount);
    mpd_del(order->taker_fee);
    mpd_del(order->maker_fee);
    mpd_del(order->left);
    mpd_del(order->freeze);
    mpd_del(order->deal_stock);
    mpd_del(order->deal_money);
    mpd_del(order->deal_fee);
    free(order->market);
    free(order->source);
    free(order);
}


json_t *get_order_info(order_t *order)
{
    json_t *info = json_object();
    json_object_set_new(info, "id", json_integer(order->id));
    json_object_set_new(info, "market", json_string(order->market));
    json_object_set_new(info, "source", json_string(order->source));
    json_object_set_new(info, "type", json_integer(order->type));
    json_object_set_new(info, "side", json_integer(order->side));
    json_object_set_new(info, "user", json_integer(order->user_id));
    json_object_set_new(info, "ctime", json_real(order->create_time));
    json_object_set_new(info, "mtime", json_real(order->update_time));

    json_object_set_new_mpd(info, "price", order->price);
    json_object_set_new_mpd(info, "amount", order->amount);
    json_object_set_new_mpd(info, "taker_fee", order->taker_fee);
    json_object_set_new_mpd(info, "maker_fee", order->maker_fee);
    json_object_set_new_mpd(info, "left", order->left);
    json_object_set_new_mpd(info, "deal_stock", order->deal_stock);
    json_object_set_new_mpd(info, "deal_money", order->deal_money);
    json_object_set_new_mpd(info, "deal_fee", order->deal_fee);

    // token discount
    json_object_set_new(info, "token", json_string(order->token));
    json_object_set_new_mpd(info, "discount", order->discount);
    json_object_set_new_mpd(info, "token_rate", order->token_rate);
    json_object_set_new_mpd(info, "asset_rate", order->asset_rate);
    json_object_set_new_mpd(info, "deal_token", order->deal_token);
    return info;
}

static int order_put(market_t *m, order_t *order)
{
    if (order->type != MARKET_ORDER_TYPE_LIMIT)
        return -__LINE__;

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

    if (order->side == MARKET_ORDER_SIDE_ASK) {
        if (skiplist_insert(m->asks, order) == NULL)
            return -__LINE__;
        mpd_copy(order->freeze, order->left, &mpd_ctx);
       // if (balance_freeze(order->user_id, m->stock, order->left) == NULL)
        //    return -__LINE__;
    } else {
        if (skiplist_insert(m->bids, order) == NULL)
            return -__LINE__;
        mpd_t *result = mpd_new(&mpd_ctx);
        mpd_mul(result, order->price, order->left, &mpd_ctx);
        mpd_copy(order->freeze, result, &mpd_ctx);
       // if (balance_freeze(order->user_id, m->money, result) == NULL) {
        //    mpd_del(result);
        //    return -__LINE__;
        //}
        mpd_del(result);
    }

    return 0;
}

static int order_finish(bool real, market_t *m, order_t *order)
{
    if (order->side == MARKET_ORDER_SIDE_ASK) {
        skiplist_node *node = skiplist_find(m->asks, order);
        if (node) {
            skiplist_delete(m->asks, node);
        }
        if (mpd_cmp(order->freeze, mpd_zero, &mpd_ctx) > 0) {
            //if (balance_unfreeze(order->user_id, m->stock, order->freeze) == NULL) {
            //    return -__LINE__;
            //}
        }
    } else {
        skiplist_node *node = skiplist_find(m->bids, order);
        if (node) {
            skiplist_delete(m->bids, node);
        }
        if (mpd_cmp(order->freeze, mpd_zero, &mpd_ctx) > 0) {
            //if (balance_unfreeze(order->user_id, m->money, order->freeze) == NULL) {
            //    return -__LINE__;
           // }
        }
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

    if (real) {
        if (mpd_cmp(order->deal_stock, mpd_zero, &mpd_ctx) > 0) {
            int ret = append_order_history(order);
            if (ret < 0) {
                log_fatal("append_order_history fail: %d, order: %"PRIu64"", ret, order->id);
            }
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
    // m->min_amount       = mpd_qncopy(conf->min_amount);
    
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

static int append_balance_trade_add(order_t *order, const char *asset, mpd_t *change, mpd_t *price, mpd_t *amount)
{
    json_t *detail = json_object();
    json_object_set_new(detail, "m", json_string(order->market));
    json_object_set_new(detail, "i", json_integer(order->id));
    json_object_set_new_mpd(detail, "p", price);
    json_object_set_new_mpd(detail, "a", amount);
    char *detail_str = json_dumps(detail, JSON_SORT_KEYS);
    int ret = append_user_balance_history(order->update_time, order->user_id, asset, "trade", change, detail_str);
    free(detail_str);
    json_decref(detail);
    return ret;
}

static int append_balance_trade_sub(order_t *order, const char *asset, mpd_t *change, mpd_t *price, mpd_t *amount)
{
    json_t *detail = json_object();
    json_object_set_new(detail, "m", json_string(order->market));
    json_object_set_new(detail, "i", json_integer(order->id));
    json_object_set_new_mpd(detail, "p", price);
    json_object_set_new_mpd(detail, "a", amount);
    char *detail_str = json_dumps(detail, JSON_SORT_KEYS);
    mpd_t *real_change = mpd_new(&mpd_ctx);
    mpd_copy_negate(real_change, change, &mpd_ctx);
    int ret = append_user_balance_history(order->update_time, order->user_id, asset, "trade", real_change, detail_str);
    mpd_del(real_change);
    free(detail_str);
    json_decref(detail);
    return ret;
}


static int append_balance_trade_fee(order_t *order, const char *asset, mpd_t *change, mpd_t *price, mpd_t *amount, mpd_t *fee_rate)
{
    json_t *detail = json_object();
    json_object_set_new(detail, "m", json_string(order->market));
    json_object_set_new(detail, "i", json_integer(order->id));
    json_object_set_new_mpd(detail, "p", price);
    json_object_set_new_mpd(detail, "a", amount);
    json_object_set_new_mpd(detail, "f", fee_rate);
    char *detail_str = json_dumps(detail, JSON_SORT_KEYS);
    mpd_t *real_change = mpd_new(&mpd_ctx);
    mpd_copy_negate(real_change, change, &mpd_ctx);
    int ret = append_user_balance_history(order->update_time, order->user_id, asset, "trade", real_change, detail_str);
    mpd_del(real_change);
    free(detail_str);
    json_decref(detail);
    return ret;
}


// token discount
static int execute_limit_ask_order(bool real, market_t *m, order_t *taker)
{
    mpd_t *price    = mpd_new(&mpd_ctx);
    mpd_t *amount   = mpd_new(&mpd_ctx);
    mpd_t *deal     = mpd_new(&mpd_ctx);
    mpd_t *ask_fee  = mpd_new(&mpd_ctx);
    mpd_t *bid_fee  = mpd_new(&mpd_ctx);
    mpd_t *result   = mpd_new(&mpd_ctx);

    mpd_t *ask_deal_token  = mpd_new(&mpd_ctx);
    mpd_t *bid_deal_token  = mpd_new(&mpd_ctx);

    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->bids);
    while ((node = skiplist_next(iter)) != NULL) {
        if (mpd_cmp(taker->left, mpd_zero, &mpd_ctx) == 0) {
            break;
        }

        order_t *maker = node->value;
        if (mpd_cmp(taker->price, maker->price, &mpd_ctx) > 0) {
            break;
        }

        mpd_copy(price, maker->price, &mpd_ctx);
        if (mpd_cmp(taker->left, maker->left, &mpd_ctx) < 0) {
            mpd_copy(amount, taker->left, &mpd_ctx);
        } else {
            mpd_copy(amount, maker->left, &mpd_ctx);
        }

        mpd_mul(deal, price, amount, &mpd_ctx);
        mpd_mul(ask_fee, deal, taker->taker_fee, &mpd_ctx);
        mpd_mul(bid_fee, amount, maker->maker_fee, &mpd_ctx);

        mpd_t *taker_balance = balance_get(taker->user_id, BALANCE_TYPE_AVAILABLE, taker->token);
        if (strlen(taker->token) == 0 || !taker_balance) {
            mpd_copy(ask_deal_token, mpd_zero, &mpd_ctx);
        } else {
            // deal_token = asset_rate / token_rate * discount * deal_fee
            mpd_mul(ask_deal_token, ask_fee, taker->asset_rate, &mpd_ctx);
            mpd_mul(ask_deal_token, ask_deal_token, taker->discount, &mpd_ctx);
            mpd_div(ask_deal_token, ask_deal_token, taker->token_rate, &mpd_ctx);
            mpd_rescale(ask_deal_token, ask_deal_token, -8, &mpd_ctx);

            if (mpd_cmp(taker_balance, ask_deal_token, &mpd_ctx) < 0) {
                mpd_sub(ask_deal_token, ask_deal_token, taker_balance, &mpd_ctx);

                mpd_mul(ask_deal_token, ask_deal_token, taker->token_rate, &mpd_ctx);
                mpd_div(ask_deal_token, ask_deal_token, taker->discount, &mpd_ctx);
                mpd_div(ask_deal_token, ask_deal_token, taker->asset_rate, &mpd_ctx);
                mpd_rescale(ask_deal_token, ask_deal_token, -8, &mpd_ctx);

                mpd_copy(ask_fee, ask_deal_token, &mpd_ctx);
                mpd_copy(ask_deal_token, taker_balance, &mpd_ctx);
            } else {
                mpd_copy(ask_fee, mpd_zero, &mpd_ctx);
            }
        }
        
        mpd_t *maker_balance = balance_get(maker->user_id, BALANCE_TYPE_AVAILABLE, maker->token);
        if (strlen(maker->token) == 0 || !maker_balance) {
            mpd_copy(bid_deal_token, mpd_zero, &mpd_ctx);
        } else {
            // deal_token = asset_rate / token_rate * discount * deal_fee

            // 如果是买单同时money = CNY, asset_rate 换成当前成交价
            if (strcmp(m->money, "CNY") == 0) {
                mpd_mul(bid_deal_token, bid_fee, price, &mpd_ctx);
            } else {
                mpd_mul(bid_deal_token, bid_fee, maker->asset_rate, &mpd_ctx);
            }

            mpd_mul(bid_deal_token, bid_deal_token, maker->discount, &mpd_ctx);
            mpd_div(bid_deal_token, bid_deal_token, maker->token_rate, &mpd_ctx);
            mpd_rescale(bid_deal_token, bid_deal_token, -8, &mpd_ctx);

            if (mpd_cmp(maker_balance, bid_deal_token, &mpd_ctx) < 0) {
                mpd_sub(bid_deal_token, bid_deal_token, maker_balance, &mpd_ctx);

                mpd_mul(bid_deal_token, bid_deal_token, maker->token_rate, &mpd_ctx);
                mpd_div(bid_deal_token, bid_deal_token, maker->discount, &mpd_ctx);
                mpd_div(bid_deal_token, bid_deal_token, maker->asset_rate, &mpd_ctx);
                mpd_rescale(bid_deal_token, bid_deal_token, -8, &mpd_ctx);

                mpd_copy(bid_fee, bid_deal_token, &mpd_ctx);
                mpd_copy(bid_deal_token, maker_balance, &mpd_ctx);
            } else {
                mpd_copy(bid_fee, mpd_zero, &mpd_ctx);
            }
        }

        taker->update_time = maker->update_time = current_timestamp();
        uint64_t deal_id = ++deals_id_start;
        if (real) {
            for (size_t i = 0; i < settings.market_num; ++i) {
                if (strcmp(m->name, settings.markets[i].name) == 0) {

                    // set default value = 0
                    if (!settings.markets[i].last)
                        settings.markets[i].last = 0;

                    mpd_copy(settings.markets[i].last, price, &mpd_ctx);
                    break;
                }
            }
            append_order_deal_history(taker->update_time, deal_id, taker, MARKET_ROLE_TAKER, maker, MARKET_ROLE_MAKER, price, amount, deal, ask_fee, bid_fee, ask_deal_token, bid_deal_token);
            push_deal_message(taker->update_time, m->name, taker, maker, price, amount, ask_fee, bid_fee, MARKET_ORDER_SIDE_ASK, deal_id, m->stock, m->money, ask_deal_token, bid_deal_token);
        }

        mpd_sub(taker->left, taker->left, amount, &mpd_ctx);
        mpd_add(taker->deal_stock, taker->deal_stock, amount, &mpd_ctx);
        mpd_add(taker->deal_money, taker->deal_money, deal, &mpd_ctx);
        mpd_add(taker->deal_fee, taker->deal_fee, ask_fee, &mpd_ctx);
        mpd_add(taker->deal_token, taker->deal_token, ask_deal_token, &mpd_ctx);

        balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_sub(taker, m->stock, amount, price, amount);
        }
        balance_add(taker->user_id, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_add(taker, m->money, deal, price, amount);
        }

        if (mpd_cmp(ask_deal_token, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, taker->token, ask_deal_token);
            if (real) {
                append_balance_trade_fee(taker, taker->token, ask_deal_token, price, amount, taker->taker_fee);
            }
        }
        if (mpd_cmp(ask_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, m->money, ask_fee);
            if (real) {
                append_balance_trade_fee(taker, m->money, ask_fee, price, amount, taker->taker_fee);
            }
        }

        mpd_sub(maker->left, maker->left, amount, &mpd_ctx);
        mpd_sub(maker->freeze, maker->freeze, deal, &mpd_ctx);
        mpd_add(maker->deal_stock, maker->deal_stock, amount, &mpd_ctx);
        mpd_add(maker->deal_money, maker->deal_money, deal, &mpd_ctx);
        mpd_add(maker->deal_fee, maker->deal_fee, bid_fee, &mpd_ctx);
        mpd_add(maker->deal_token, maker->deal_token, bid_deal_token, &mpd_ctx);

        balance_sub(maker->user_id, BALANCE_TYPE_FREEZE, m->money, deal);
        if (real) {
            append_balance_trade_sub(maker, m->money, deal, price, amount);
        }
        balance_add(maker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_add(maker, m->stock, amount, price, amount);
        }

        if (mpd_cmp(bid_deal_token, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(maker->user_id, BALANCE_TYPE_AVAILABLE, maker->token, bid_deal_token);
            if (real) {
                append_balance_trade_fee(maker, maker->token, bid_deal_token, price, amount, maker->maker_fee);
            }
        }
        if (mpd_cmp(bid_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(maker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, bid_fee);
            if (real) {
                append_balance_trade_fee(maker, m->stock, bid_fee, price, amount, maker->maker_fee);
            }
        }

        if (mpd_cmp(maker->left, mpd_zero, &mpd_ctx) == 0) {
            if (real) {
                push_order_message(ORDER_EVENT_FINISH, maker, m);
            }
            order_finish(real, m, maker);
        } else {
            if (real) {
                push_order_message(ORDER_EVENT_UPDATE, maker, m);
            }
        }
    }
    skiplist_release_iterator(iter);

    mpd_del(amount);
    mpd_del(price);
    mpd_del(deal);
    mpd_del(ask_fee);
    mpd_del(bid_fee);
    mpd_del(ask_deal_token);
    mpd_del(bid_deal_token);
    mpd_del(result);

    return 0;
}


// token discount
static int execute_limit_bid_order(bool real, market_t *m, order_t *taker)
{
    mpd_t *price    = mpd_new(&mpd_ctx);
    mpd_t *amount   = mpd_new(&mpd_ctx);
    mpd_t *deal     = mpd_new(&mpd_ctx);
    mpd_t *ask_fee  = mpd_new(&mpd_ctx);
    mpd_t *bid_fee  = mpd_new(&mpd_ctx);
    mpd_t *result   = mpd_new(&mpd_ctx);

    mpd_t *ask_deal_token  = mpd_new(&mpd_ctx);
    mpd_t *bid_deal_token  = mpd_new(&mpd_ctx);

    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->asks);
    while ((node = skiplist_next(iter)) != NULL) {
        if (mpd_cmp(taker->left, mpd_zero, &mpd_ctx) == 0) {
            break;
        }

        order_t *maker = node->value;
        if (mpd_cmp(taker->price, maker->price, &mpd_ctx) < 0) {
            break;
        }

        mpd_copy(price, maker->price, &mpd_ctx);
        if (mpd_cmp(taker->left, maker->left, &mpd_ctx) < 0) {
            mpd_copy(amount, taker->left, &mpd_ctx);
        } else {
            mpd_copy(amount, maker->left, &mpd_ctx);
        }

        mpd_mul(deal, price, amount, &mpd_ctx);
        mpd_mul(ask_fee, deal, maker->maker_fee, &mpd_ctx);
        mpd_mul(bid_fee, amount, taker->taker_fee, &mpd_ctx);

        mpd_t *taker_balance = balance_get(taker->user_id, BALANCE_TYPE_AVAILABLE, taker->token);
        if (strlen(taker->token) == 0 || !taker_balance) {
            mpd_copy(bid_deal_token, mpd_zero, &mpd_ctx);
        } else {
            // deal_token = asset_rate / token_rate * discount * deal_fee

            // 如果是买单同时money = CNY, asset_rate 换成当前成交价
            if (strcmp(m->money, "CNY") == 0) {
                mpd_mul(bid_deal_token, bid_fee, price, &mpd_ctx);
            } else {
                mpd_mul(bid_deal_token, bid_fee, taker->asset_rate, &mpd_ctx);
            }

            // mpd_mul(bid_deal_token, bid_fee, taker->asset_rate, &mpd_ctx);
            mpd_mul(bid_deal_token, bid_deal_token, taker->discount, &mpd_ctx);
            mpd_div(bid_deal_token, bid_deal_token, taker->token_rate, &mpd_ctx);
            mpd_rescale(bid_deal_token, bid_deal_token, -8, &mpd_ctx);

            if (mpd_cmp(taker_balance, bid_deal_token, &mpd_ctx) < 0) {
                mpd_sub(bid_deal_token, bid_deal_token, taker_balance, &mpd_ctx);

                mpd_mul(bid_deal_token, bid_deal_token, taker->token_rate, &mpd_ctx);
                mpd_div(bid_deal_token, bid_deal_token, taker->discount, &mpd_ctx);
                mpd_div(bid_deal_token, bid_deal_token, taker->asset_rate, &mpd_ctx);
                mpd_rescale(bid_deal_token, bid_deal_token, -8, &mpd_ctx);

                mpd_copy(bid_fee, bid_deal_token, &mpd_ctx);
                mpd_copy(bid_deal_token, taker_balance, &mpd_ctx);
            } else {
                mpd_copy(bid_fee, mpd_zero, &mpd_ctx);
            }
        }

        mpd_t *maker_balance = balance_get(maker->user_id, BALANCE_TYPE_AVAILABLE, maker->token);
        if (strlen(maker->token) == 0 || !maker_balance) {
            mpd_copy(ask_deal_token, mpd_zero, &mpd_ctx);
        } else {
            // deal_token = asset_rate / token_rate * discount * deal_fee
            mpd_mul(ask_deal_token, ask_fee, maker->asset_rate, &mpd_ctx);
            mpd_mul(ask_deal_token, ask_deal_token, maker->discount, &mpd_ctx);
            mpd_div(ask_deal_token, ask_deal_token, maker->token_rate, &mpd_ctx);
            mpd_rescale(ask_deal_token, ask_deal_token, -8, &mpd_ctx);

            if (mpd_cmp(maker_balance, ask_deal_token, &mpd_ctx) < 0) {
                mpd_sub(ask_deal_token, ask_deal_token, maker_balance, &mpd_ctx);

                mpd_mul(ask_deal_token, ask_deal_token, maker->token_rate, &mpd_ctx);
                mpd_div(ask_deal_token, ask_deal_token, maker->discount, &mpd_ctx);
                mpd_div(ask_deal_token, ask_deal_token, maker->asset_rate, &mpd_ctx);
                mpd_rescale(ask_deal_token, ask_deal_token, -8, &mpd_ctx);

                mpd_copy(ask_fee, ask_deal_token, &mpd_ctx);
                mpd_copy(ask_deal_token, maker_balance, &mpd_ctx);
            } else {
                mpd_copy(ask_fee, mpd_zero, &mpd_ctx);
            }
        }

        taker->update_time = maker->update_time = current_timestamp();
        uint64_t deal_id = ++deals_id_start;
        if (real) {
            for (size_t i = 0; i < settings.market_num; ++i) {
                if (strcmp(m->name, settings.markets[i].name) == 0) {

                    // set default value = 0
                    if (!settings.markets[i].last)
                        settings.markets[i].last = 0;
                    
                    mpd_copy(settings.markets[i].last, price, &mpd_ctx);
                    break;
                }
            }
            append_order_deal_history(taker->update_time, deal_id, maker, MARKET_ROLE_MAKER, taker, MARKET_ROLE_TAKER, price, amount, deal, ask_fee, bid_fee, ask_deal_token, bid_deal_token);
            push_deal_message(taker->update_time, m->name, maker, taker, price, amount, ask_fee, bid_fee, MARKET_ORDER_SIDE_BID, deal_id, m->stock, m->money, ask_deal_token, bid_deal_token);
        }

        mpd_sub(taker->left, taker->left, amount, &mpd_ctx);
        mpd_add(taker->deal_stock, taker->deal_stock, amount, &mpd_ctx);
        mpd_add(taker->deal_money, taker->deal_money, deal, &mpd_ctx);
        mpd_add(taker->deal_fee, taker->deal_fee, bid_fee, &mpd_ctx);
        mpd_add(taker->deal_token, taker->deal_token, bid_deal_token, &mpd_ctx);

        balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_sub(taker, m->money, deal, price, amount);
        }
        balance_add(taker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_add(taker, m->stock, amount, price, amount);
        }

        if (mpd_cmp(bid_deal_token, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, taker->token, bid_deal_token);
            if (real) {
                append_balance_trade_fee(taker, taker->token, bid_deal_token, price, amount, taker->taker_fee);
            }
        }
        if (mpd_cmp(bid_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, bid_fee);
            if (real) {
                append_balance_trade_fee(taker, m->stock, bid_fee, price, amount, taker->taker_fee);
            }
        }

        mpd_sub(maker->left, maker->left, amount, &mpd_ctx);
        mpd_sub(maker->freeze, maker->freeze, amount, &mpd_ctx);
        mpd_add(maker->deal_stock, maker->deal_stock, amount, &mpd_ctx);
        mpd_add(maker->deal_money, maker->deal_money, deal, &mpd_ctx);
        mpd_add(maker->deal_fee, maker->deal_fee, ask_fee, &mpd_ctx);
        mpd_add(maker->deal_token, maker->deal_token, ask_deal_token, &mpd_ctx);

        balance_sub(maker->user_id, BALANCE_TYPE_FREEZE, m->stock, amount);
        if (real) {
            append_balance_trade_sub(maker, m->stock, amount, price, amount);
        }
        balance_add(maker->user_id, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_add(maker, m->money, deal, price, amount);
        }

        if (mpd_cmp(ask_deal_token, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(maker->user_id, BALANCE_TYPE_AVAILABLE, maker->token, ask_deal_token);
            if (real) {
                append_balance_trade_fee(maker, maker->token, ask_deal_token, price, amount, maker->maker_fee);
            }
        }
        if (mpd_cmp(ask_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(maker->user_id, BALANCE_TYPE_AVAILABLE, m->money, ask_fee);
            if (real) {
                append_balance_trade_fee(maker, m->money, ask_fee, price, amount, maker->maker_fee);
            }
        }

        if (mpd_cmp(maker->left, mpd_zero, &mpd_ctx) == 0) {
            if (real) {
                push_order_message(ORDER_EVENT_FINISH, maker, m);
            }
            order_finish(real, m, maker);
        } else {
            if (real) {
                push_order_message(ORDER_EVENT_UPDATE, maker, m);
            }
        }
    }
    skiplist_release_iterator(iter);

    mpd_del(amount);
    mpd_del(price);
    mpd_del(deal);
    mpd_del(ask_fee);
    mpd_del(bid_fee);
    mpd_del(ask_deal_token);
    mpd_del(bid_deal_token);
    mpd_del(result);

    return 0;
}


// token discount
int market_put_limit_order(bool real, json_t **result, market_t *m, uint32_t user_id, uint32_t side, mpd_t *amount, mpd_t *price,
                           mpd_t *taker_fee, mpd_t *maker_fee, const char *source, char* token, mpd_t *discount, mpd_t *token_rate, mpd_t *asset_rate)
{
    if (strlen(token) != 0 && mpd_cmp(token_rate, mpd_zero, &mpd_ctx) <= 0)
        return -4;
    if (strlen(token) != 0 && mpd_cmp(asset_rate, mpd_zero, &mpd_ctx) <= 0)
        return -4;

    if (side == MARKET_ORDER_SIDE_ASK) {
        mpd_t *balance = balance_get(user_id, BALANCE_TYPE_AVAILABLE, m->stock);
        if (!balance || mpd_cmp(balance, amount, &mpd_ctx) < 0) {
            return -1;
        }
    } else {
        mpd_t *balance = balance_get(user_id, BALANCE_TYPE_AVAILABLE, m->money);
        mpd_t *require = mpd_new(&mpd_ctx);
        mpd_mul(require, amount, price, &mpd_ctx);
        if (!balance || mpd_cmp(balance, require, &mpd_ctx) < 0) {
            mpd_del(require);
            return -1;
        }
        mpd_del(require);
    }

    if (mpd_cmp(amount, m->min_amount, &mpd_ctx) < 0) {
        return -2;
    }

    order_t *order = malloc(sizeof(order_t));
    if (order == NULL) {
        return -__LINE__;
    }

    order->id           = ++order_id_start;
    order->type         = MARKET_ORDER_TYPE_LIMIT;
    order->side         = side;
    order->create_time  = current_timestamp();
    order->update_time  = order->create_time;
    order->market       = strdup(m->name);
    order->source       = strdup(source);
    order->user_id      = user_id;
    order->price        = mpd_new(&mpd_ctx);
    order->amount       = mpd_new(&mpd_ctx);
    order->taker_fee    = mpd_new(&mpd_ctx);
    order->maker_fee    = mpd_new(&mpd_ctx);
    order->left         = mpd_new(&mpd_ctx);
    order->freeze       = mpd_new(&mpd_ctx);
    order->deal_stock   = mpd_new(&mpd_ctx);
    order->deal_money   = mpd_new(&mpd_ctx);
    order->deal_fee     = mpd_new(&mpd_ctx);
    order->token        = strdup(token);
    order->token_rate   = mpd_new(&mpd_ctx);
    order->asset_rate   = mpd_new(&mpd_ctx);
    order->discount     = mpd_new(&mpd_ctx);
    order->deal_token   = mpd_new(&mpd_ctx);

    mpd_copy(order->price, price, &mpd_ctx);
    mpd_copy(order->amount, amount, &mpd_ctx);
    mpd_copy(order->taker_fee, taker_fee, &mpd_ctx);
    mpd_copy(order->maker_fee, maker_fee, &mpd_ctx);
    mpd_copy(order->left, amount, &mpd_ctx);
    mpd_copy(order->freeze, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_stock, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_money, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_fee, mpd_zero, &mpd_ctx);
    mpd_copy(order->discount, discount, &mpd_ctx);
    mpd_copy(order->token_rate, token_rate, &mpd_ctx);
    mpd_copy(order->asset_rate, asset_rate, &mpd_ctx);
    mpd_copy(order->deal_token, mpd_zero, &mpd_ctx);

    int ret;
    if (side == MARKET_ORDER_SIDE_ASK) {
        ret = execute_limit_ask_order(real, m, order);
    } else {
        ret = execute_limit_bid_order(real, m, order);
    }
    if (ret < 0) {
        log_error("execute order: %"PRIu64" fail: %d", order->id, ret);
        order_free(order);
        return -__LINE__;
    }

    if (mpd_cmp(order->left, mpd_zero, &mpd_ctx) == 0) {
        if (real) {
            ret = append_order_history(order);
            if (ret < 0) {
                log_fatal("append_order_history fail: %d, order: %"PRIu64"", ret, order->id);
            }
            push_order_message(ORDER_EVENT_FINISH, order, m);
            *result = get_order_info(order);
        }
        order_free(order);
    } else {
        if (real) {
            push_order_message(ORDER_EVENT_PUT, order, m);
            *result = get_order_info(order);
        }
        ret = order_put(m, order);
        if (ret < 0) {
            log_fatal("order_put fail: %d, order: %"PRIu64"", ret, order->id);
        }
    }

    return 0;
}


// token discount
static int execute_market_ask_order(bool real, market_t *m, order_t *taker)
{
    mpd_t *price    = mpd_new(&mpd_ctx);
    mpd_t *amount   = mpd_new(&mpd_ctx);
    mpd_t *deal     = mpd_new(&mpd_ctx);
    mpd_t *ask_fee  = mpd_new(&mpd_ctx);
    mpd_t *bid_fee  = mpd_new(&mpd_ctx);
    mpd_t *result   = mpd_new(&mpd_ctx);

    mpd_t *ask_deal_token  = mpd_new(&mpd_ctx);
    mpd_t *bid_deal_token  = mpd_new(&mpd_ctx);

    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->bids);
    while ((node = skiplist_next(iter)) != NULL) {
        if (mpd_cmp(taker->left, mpd_zero, &mpd_ctx) == 0) {
            break;
        }

        order_t *maker = node->value;
        mpd_copy(price, maker->price, &mpd_ctx);
        if (mpd_cmp(taker->left, maker->left, &mpd_ctx) < 0) {
            mpd_copy(amount, taker->left, &mpd_ctx);
        } else {
            mpd_copy(amount, maker->left, &mpd_ctx);
        }

        mpd_mul(deal, price, amount, &mpd_ctx);
        mpd_mul(ask_fee, deal, taker->taker_fee, &mpd_ctx);
        mpd_mul(bid_fee, amount, maker->maker_fee, &mpd_ctx);

        mpd_t *taker_balance = balance_get(taker->user_id, BALANCE_TYPE_AVAILABLE, taker->token);
        if (strlen(taker->token) == 0 || !taker_balance) {
            mpd_copy(ask_deal_token, mpd_zero, &mpd_ctx);
        } else {
            // deal_token = asset_rate / token_rate * discount * deal_fee
            mpd_mul(ask_deal_token, ask_fee, taker->asset_rate, &mpd_ctx);
            mpd_mul(ask_deal_token, ask_deal_token, taker->discount, &mpd_ctx);
            mpd_div(ask_deal_token, ask_deal_token, taker->token_rate, &mpd_ctx);
            mpd_rescale(ask_deal_token, ask_deal_token, -8, &mpd_ctx);

            if (mpd_cmp(taker_balance, ask_deal_token, &mpd_ctx) < 0) {
                mpd_sub(ask_deal_token, ask_deal_token, taker_balance, &mpd_ctx);

                mpd_mul(ask_deal_token, ask_deal_token, taker->token_rate, &mpd_ctx);
                mpd_div(ask_deal_token, ask_deal_token, taker->discount, &mpd_ctx);
                mpd_div(ask_deal_token, ask_deal_token, taker->asset_rate, &mpd_ctx);
                mpd_rescale(ask_deal_token, ask_deal_token, -8, &mpd_ctx);

                mpd_copy(ask_fee, ask_deal_token, &mpd_ctx);
                mpd_copy(ask_deal_token, taker_balance, &mpd_ctx);
            } else {
                mpd_copy(ask_fee, mpd_zero, &mpd_ctx);
            }
        }

        mpd_t *maker_balance = balance_get(taker->user_id, BALANCE_TYPE_AVAILABLE, maker->token);
        if (strlen(maker->token) == 0 || !maker_balance) {
            mpd_copy(bid_deal_token, mpd_zero, &mpd_ctx);
        } else {
            // deal_token = asset_rate / token_rate * discount * deal_fee

            // 如果是买单同时money = CNY, asset_rate 换成当前成交价
            if (strcmp(m->money, "CNY") == 0) {
                mpd_mul(bid_deal_token, bid_fee, price, &mpd_ctx);
            } else {
                mpd_mul(bid_deal_token, bid_fee, maker->asset_rate, &mpd_ctx);
            }

            // mpd_mul(bid_deal_token, bid_fee, maker->asset_rate, &mpd_ctx);
            mpd_mul(bid_deal_token, bid_deal_token, maker->discount, &mpd_ctx);
            mpd_div(bid_deal_token, bid_deal_token, maker->token_rate, &mpd_ctx);
            mpd_rescale(bid_deal_token, bid_deal_token, -8, &mpd_ctx);

            if (mpd_cmp(maker_balance, bid_deal_token, &mpd_ctx) < 0) {
                mpd_sub(bid_deal_token, bid_deal_token, maker_balance, &mpd_ctx);

                mpd_mul(bid_deal_token, bid_deal_token, maker->token_rate, &mpd_ctx);
                mpd_div(bid_deal_token, bid_deal_token, maker->discount, &mpd_ctx);
                mpd_div(bid_deal_token, bid_deal_token, maker->asset_rate, &mpd_ctx);
                mpd_rescale(bid_deal_token, bid_deal_token, -8, &mpd_ctx);

                mpd_copy(bid_fee, bid_deal_token, &mpd_ctx);
                mpd_copy(bid_deal_token, maker_balance, &mpd_ctx);
            } else {
                mpd_copy(bid_fee, mpd_zero, &mpd_ctx);
            }
        }

        taker->update_time = maker->update_time = current_timestamp();
        uint64_t deal_id = ++deals_id_start;
        if (real) {
            for (size_t i = 0; i < settings.market_num; ++i) {
                if (strcmp(m->name, settings.markets[i].name) == 0) {

                    // set default value = 0
                    if (!settings.markets[i].last)
                        settings.markets[i].last = 0;

                    mpd_copy(settings.markets[i].last, price, &mpd_ctx);
                    break;
                }
            }
            append_order_deal_history(taker->update_time, deal_id, taker, MARKET_ROLE_TAKER, maker, MARKET_ROLE_MAKER, price, amount, deal, ask_fee, bid_fee, ask_deal_token, bid_deal_token);
            push_deal_message(taker->update_time, m->name, taker, maker, price, amount, ask_fee, bid_fee, MARKET_ORDER_SIDE_ASK, deal_id, m->stock, m->money, ask_deal_token, bid_deal_token);
        }

        mpd_sub(taker->left, taker->left, amount, &mpd_ctx);
        mpd_add(taker->deal_stock, taker->deal_stock, amount, &mpd_ctx);
        mpd_add(taker->deal_money, taker->deal_money, deal, &mpd_ctx);
        mpd_add(taker->deal_fee, taker->deal_fee, ask_fee, &mpd_ctx);
        mpd_add(taker->deal_token, taker->deal_token, ask_deal_token, &mpd_ctx);

        balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_sub(taker, m->stock, amount, price, amount);
        }
        balance_add(taker->user_id, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_add(taker, m->money, deal, price, amount);
        }

        if (mpd_cmp(ask_deal_token, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, taker->token, ask_deal_token);
            if (real) {
                append_balance_trade_fee(taker, taker->token, ask_deal_token, price, amount, taker->taker_fee);
            }
        }
        if (mpd_cmp(ask_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, m->money, ask_fee);
            if (real) {
                append_balance_trade_fee(taker, m->money, ask_fee, price, amount, taker->taker_fee);
            }
        }

        mpd_sub(maker->left, maker->left, amount, &mpd_ctx);
        mpd_sub(maker->freeze, maker->freeze, deal, &mpd_ctx);
        mpd_add(maker->deal_stock, maker->deal_stock, amount, &mpd_ctx);
        mpd_add(maker->deal_money, maker->deal_money, deal, &mpd_ctx);
        mpd_add(maker->deal_fee, maker->deal_fee, bid_fee, &mpd_ctx);
        mpd_add(maker->deal_token, maker->deal_token, bid_deal_token, &mpd_ctx);

        balance_sub(maker->user_id, BALANCE_TYPE_FREEZE, m->money, deal);
        if (real) {
            append_balance_trade_sub(maker, m->money, deal, price, amount);
        }
        balance_add(maker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_add(maker, m->stock, amount, price, amount);
        }

        if (mpd_cmp(bid_deal_token, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(maker->user_id, BALANCE_TYPE_AVAILABLE, maker->token, bid_deal_token);
            if (real) {
                append_balance_trade_fee(maker, maker->token, bid_deal_token, price, amount, maker->maker_fee);
            }
        }
        if (mpd_cmp(bid_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(maker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, bid_fee);
            if (real) {
                append_balance_trade_fee(maker, m->stock, bid_fee, price, amount, maker->maker_fee);
            }
        }

        if (mpd_cmp(maker->left, mpd_zero, &mpd_ctx) == 0) {
            if (real) {
                push_order_message(ORDER_EVENT_FINISH, maker, m);
            }
            order_finish(real, m, maker);
        } else {
            if (real) {
                push_order_message(ORDER_EVENT_UPDATE, maker, m);
            }
        }
    }
    skiplist_release_iterator(iter);

    mpd_del(amount);
    mpd_del(price);
    mpd_del(deal);
    mpd_del(ask_fee);
    mpd_del(bid_fee);
    mpd_del(ask_deal_token);
    mpd_del(bid_deal_token);
    mpd_del(result);

    return 0;
}


// token discount
static int execute_market_bid_order(bool real, market_t *m, order_t *taker)
{
    mpd_t *price    = mpd_new(&mpd_ctx);
    mpd_t *amount   = mpd_new(&mpd_ctx);
    mpd_t *deal     = mpd_new(&mpd_ctx);
    mpd_t *ask_fee  = mpd_new(&mpd_ctx);
    mpd_t *bid_fee  = mpd_new(&mpd_ctx);
    mpd_t *result   = mpd_new(&mpd_ctx);

    mpd_t *ask_deal_token  = mpd_new(&mpd_ctx);
    mpd_t *bid_deal_token  = mpd_new(&mpd_ctx);

    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->asks);
    while ((node = skiplist_next(iter)) != NULL) {
        if (mpd_cmp(taker->left, mpd_zero, &mpd_ctx) == 0) {
            break;
        }

        order_t *maker = node->value;
        mpd_copy(price, maker->price, &mpd_ctx);

        mpd_div(amount, taker->left, price, &mpd_ctx);
        mpd_rescale(amount, amount, -m->stock_prec, &mpd_ctx);
        while (true) {
            mpd_mul(result, amount, price, &mpd_ctx);
            if (mpd_cmp(result, taker->left, &mpd_ctx) > 0) {
                mpd_set_i32(result, -m->stock_prec, &mpd_ctx);
                mpd_pow(result, mpd_ten, result, &mpd_ctx);
                mpd_sub(amount, amount, result, &mpd_ctx);
            } else {
                break;
            }
        }

        if (mpd_cmp(amount, maker->left, &mpd_ctx) > 0) {
            mpd_copy(amount, maker->left, &mpd_ctx);
        }
        if (mpd_cmp(amount, mpd_zero, &mpd_ctx) == 0) {
            break;
        }

        mpd_mul(deal, price, amount, &mpd_ctx);
        mpd_mul(ask_fee, deal, maker->maker_fee, &mpd_ctx);
        mpd_mul(bid_fee, amount, taker->taker_fee, &mpd_ctx);

        mpd_t *taker_balance = balance_get(taker->user_id, BALANCE_TYPE_AVAILABLE, taker->token);
        if (strlen(taker->token) == 0 || !taker_balance) {
            mpd_copy(bid_deal_token, mpd_zero, &mpd_ctx);
        } else {
            // deal_token = asset_rate / token_rate * discount * deal_fee

            // 如果是买单同时money = CNY, asset_rate 换成当前成交价
            if (strcmp(m->money, "CNY") == 0) {
                mpd_mul(bid_deal_token, bid_fee, price, &mpd_ctx);
            } else {
                mpd_mul(bid_deal_token, bid_fee, taker->asset_rate, &mpd_ctx);
            }

            // mpd_mul(bid_deal_token, bid_fee, taker->asset_rate, &mpd_ctx);
            mpd_mul(bid_deal_token, bid_deal_token, taker->discount, &mpd_ctx);
            mpd_div(bid_deal_token, bid_deal_token, taker->token_rate, &mpd_ctx);
            mpd_rescale(bid_deal_token, bid_deal_token, -8, &mpd_ctx);

            if (mpd_cmp(taker_balance, bid_deal_token, &mpd_ctx) < 0) {
                mpd_sub(bid_deal_token, bid_deal_token, taker_balance, &mpd_ctx);

                mpd_mul(bid_deal_token, bid_deal_token, taker->token_rate, &mpd_ctx);
                mpd_div(bid_deal_token, bid_deal_token, taker->discount, &mpd_ctx);
                mpd_div(bid_deal_token, bid_deal_token, taker->asset_rate, &mpd_ctx);
                mpd_rescale(bid_deal_token, bid_deal_token, -8, &mpd_ctx);

                mpd_copy(bid_fee, bid_deal_token, &mpd_ctx);
                mpd_copy(bid_deal_token, taker_balance, &mpd_ctx);
            } else {
                mpd_copy(bid_fee, mpd_zero, &mpd_ctx);
            }
        }

        mpd_t *maker_balance = balance_get(maker->user_id, BALANCE_TYPE_AVAILABLE, maker->token);
        if (strlen(maker->token) == 0 || !maker_balance) {
            mpd_copy(ask_deal_token, mpd_zero, &mpd_ctx);
        } else {
            // deal_token = asset_rate / token_rate * discount * deal_fee
            mpd_mul(ask_deal_token, ask_fee, maker->asset_rate, &mpd_ctx);
            mpd_mul(ask_deal_token, ask_deal_token, maker->discount, &mpd_ctx);
            mpd_div(ask_deal_token, ask_deal_token, maker->token_rate, &mpd_ctx);
            mpd_rescale(ask_deal_token, ask_deal_token, -8, &mpd_ctx);

            if (mpd_cmp(maker_balance, ask_deal_token, &mpd_ctx) < 0) {
                mpd_sub(ask_deal_token, ask_deal_token, maker_balance, &mpd_ctx);

                mpd_mul(ask_deal_token, ask_deal_token, maker->token_rate, &mpd_ctx);
                mpd_div(ask_deal_token, ask_deal_token, maker->discount, &mpd_ctx);
                mpd_div(ask_deal_token, ask_deal_token, maker->asset_rate, &mpd_ctx);
                mpd_rescale(ask_deal_token, ask_deal_token, -8, &mpd_ctx);

                mpd_copy(ask_fee, ask_deal_token, &mpd_ctx);
                mpd_copy(ask_deal_token, maker_balance, &mpd_ctx);
            } else {
                mpd_copy(ask_fee, mpd_zero, &mpd_ctx);
            }
        }

        taker->update_time = maker->update_time = current_timestamp();
        uint64_t deal_id = ++deals_id_start;
        if (real) {
            for (size_t i = 0; i < settings.market_num; ++i) {
                if (strcmp(m->name, settings.markets[i].name) == 0) {

                    // set default value = 0
                    if (!settings.markets[i].last)
                        settings.markets[i].last = 0;

                    mpd_copy(settings.markets[i].last, price, &mpd_ctx);
                    break;
                }
            }
            append_order_deal_history(taker->update_time, deal_id, maker, MARKET_ROLE_MAKER, taker, MARKET_ROLE_TAKER, price, amount, deal, ask_fee, bid_fee, ask_deal_token, bid_deal_token);
            push_deal_message(taker->update_time, m->name, maker, taker, price, amount, ask_fee, bid_fee, MARKET_ORDER_SIDE_BID, deal_id, m->stock, m->money, ask_deal_token, bid_deal_token);
        }

        mpd_sub(taker->left, taker->left, deal, &mpd_ctx);
        mpd_add(taker->deal_stock, taker->deal_stock, amount, &mpd_ctx);
        mpd_add(taker->deal_money, taker->deal_money, deal, &mpd_ctx);
        mpd_add(taker->deal_fee, taker->deal_fee, bid_fee, &mpd_ctx);
        mpd_add(taker->deal_token, taker->deal_token, bid_deal_token, &mpd_ctx);

        balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_sub(taker, m->money, deal, price, amount);
        }
        balance_add(taker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_add(taker, m->stock, amount, price, amount);
        }

        if (mpd_cmp(bid_deal_token, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, taker->token, bid_deal_token);
            if (real) {
                append_balance_trade_fee(taker, taker->token, bid_deal_token, price, amount, taker->taker_fee);
            }
        }
        if (mpd_cmp(bid_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, bid_fee);
            if (real) {
                append_balance_trade_fee(taker, m->stock, bid_fee, price, amount, taker->taker_fee);
            }
        }

        mpd_sub(maker->left, maker->left, amount, &mpd_ctx);
        mpd_sub(maker->freeze, maker->freeze, amount, &mpd_ctx);
        mpd_add(maker->deal_stock, maker->deal_stock, amount, &mpd_ctx);
        mpd_add(maker->deal_money, maker->deal_money, deal, &mpd_ctx);
        mpd_add(maker->deal_fee, maker->deal_fee, ask_fee, &mpd_ctx);
        mpd_add(maker->deal_token, maker->deal_token, ask_deal_token, &mpd_ctx);

        balance_sub(maker->user_id, BALANCE_TYPE_FREEZE, m->stock, amount);
        if (real) {
            append_balance_trade_sub(maker, m->stock, amount, price, amount);
        }
        balance_add(maker->user_id, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_add(maker, m->money, deal, price, amount);
        }

        if (mpd_cmp(ask_deal_token, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(maker->user_id, BALANCE_TYPE_AVAILABLE, maker->token, ask_deal_token);
            if (real) {
                append_balance_trade_fee(maker, maker->token, ask_deal_token, price, amount, maker->maker_fee);
            }
        }
        if (mpd_cmp(ask_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(maker->user_id, BALANCE_TYPE_AVAILABLE, m->money, ask_fee);
            if (real) {
                append_balance_trade_fee(maker, m->money, ask_fee, price, amount, maker->maker_fee);
            }
        }

        if (mpd_cmp(maker->left, mpd_zero, &mpd_ctx) == 0) {
            if (real) {
                push_order_message(ORDER_EVENT_FINISH, maker, m);
            }
            order_finish(real, m, maker);
        } else {
            if (real) {
                push_order_message(ORDER_EVENT_UPDATE, maker, m);
            }
        }
    }

    skiplist_release_iterator(iter);

    mpd_del(amount);
    mpd_del(price);
    mpd_del(deal);
    mpd_del(ask_fee);
    mpd_del(bid_fee);
    mpd_del(ask_deal_token);
    mpd_del(bid_deal_token);
    mpd_del(result);

    return 0;
}


// token discount
int market_put_market_order(bool real, json_t **result, market_t *m, uint32_t user_id, uint32_t side, mpd_t *amount, 
                            mpd_t *taker_fee, const char *source, char* token, mpd_t *discount, mpd_t *token_rate, mpd_t *asset_rate)
{
    if (strlen(token) != 0 && mpd_cmp(token_rate, mpd_zero, &mpd_ctx) <= 0)
        return -4;
    if (strlen(token) != 0 && mpd_cmp(asset_rate, mpd_zero, &mpd_ctx) <= 0)
        return -4;

    if (side == MARKET_ORDER_SIDE_ASK) {
        mpd_t *balance = balance_get(user_id, BALANCE_TYPE_AVAILABLE, m->stock);
        if (!balance || mpd_cmp(balance, amount, &mpd_ctx) < 0) {
            return -1;
        }

        skiplist_iter *iter = skiplist_get_iterator(m->bids);
        skiplist_node *node = skiplist_next(iter);
        if (node == NULL) {
            skiplist_release_iterator(iter);
            return -3;
        }
        skiplist_release_iterator(iter);

        if (mpd_cmp(amount, m->min_amount, &mpd_ctx) < 0) {
            return -2;
        }
    } else {
        mpd_t *balance = balance_get(user_id, BALANCE_TYPE_AVAILABLE, m->money);
        if (!balance || mpd_cmp(balance, amount, &mpd_ctx) < 0) {
            return -1;
        }

        skiplist_iter *iter = skiplist_get_iterator(m->asks);
        skiplist_node *node = skiplist_next(iter);
        if (node == NULL) {
            skiplist_release_iterator(iter);
            return -3;
        }
        skiplist_release_iterator(iter);

        order_t *order = node->value;
        mpd_t *require = mpd_new(&mpd_ctx);
        mpd_mul(require, order->price, m->min_amount, &mpd_ctx);
        if (mpd_cmp(amount, require, &mpd_ctx) < 0) {
            mpd_del(require);
            return -2;
        }
        mpd_del(require);
    }

    order_t *order = malloc(sizeof(order_t));
    if (order == NULL) {
        return -__LINE__;
    }

    order->id           = ++order_id_start;
    order->type         = MARKET_ORDER_TYPE_MARKET;
    order->side         = side;
    order->create_time  = current_timestamp();
    order->update_time  = order->create_time;
    order->market       = strdup(m->name);
    order->source       = strdup(source);
    order->user_id      = user_id;
    order->price        = mpd_new(&mpd_ctx);
    order->amount       = mpd_new(&mpd_ctx);
    order->taker_fee    = mpd_new(&mpd_ctx);
    order->maker_fee    = mpd_new(&mpd_ctx);
    order->left         = mpd_new(&mpd_ctx);
    order->freeze       = mpd_new(&mpd_ctx);
    order->deal_stock   = mpd_new(&mpd_ctx);
    order->deal_money   = mpd_new(&mpd_ctx);
    order->deal_fee     = mpd_new(&mpd_ctx);
    order->token        = strdup(token);
    order->token_rate   = mpd_new(&mpd_ctx);
    order->asset_rate   = mpd_new(&mpd_ctx);
    order->discount     = mpd_new(&mpd_ctx);
    order->deal_token   = mpd_new(&mpd_ctx);

    mpd_copy(order->price, mpd_zero, &mpd_ctx);
    mpd_copy(order->amount, amount, &mpd_ctx);
    mpd_copy(order->taker_fee, taker_fee, &mpd_ctx);
    mpd_copy(order->maker_fee, mpd_zero, &mpd_ctx);
    mpd_copy(order->left, amount, &mpd_ctx);
    mpd_copy(order->freeze, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_stock, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_money, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_fee, mpd_zero, &mpd_ctx);
    mpd_copy(order->discount, discount, &mpd_ctx);
    mpd_copy(order->token_rate, token_rate, &mpd_ctx);
    mpd_copy(order->asset_rate, asset_rate, &mpd_ctx);
    mpd_copy(order->deal_token, mpd_zero, &mpd_ctx);

    int ret;
    if (side == MARKET_ORDER_SIDE_ASK) {
        ret = execute_market_ask_order(real, m, order);
    } else {
        ret = execute_market_bid_order(real, m, order);
    }
    if (ret < 0) {
        log_error("execute order: %"PRIu64" fail: %d", order->id, ret);
        order_free(order);
        return -__LINE__;
    }

    if (real) {
        int ret = append_order_history(order);
        if (ret < 0) {
            log_fatal("append_order_history fail: %d, order: %"PRIu64"", ret, order->id);
        }
        push_order_message(ORDER_EVENT_FINISH, order, m);
        *result = get_order_info(order);
    }

    order_free(order);
    return 0;
}


int market_cancel_order(bool real, json_t **result, market_t *m, order_t *order)
{
    if (real) {
        push_order_message(ORDER_EVENT_FINISH, order, m);
        *result = get_order_info(order);
    }
    order_finish(real, m, order);
    return 0;
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

int market_get_status(market_t *m, size_t *ask_count, mpd_t *ask_amount, size_t *bid_count, mpd_t *bid_amount)
{
    *ask_count = m->asks->len;
    *bid_count = m->bids->len;
    mpd_copy(ask_amount, mpd_zero, &mpd_ctx);
    mpd_copy(bid_amount, mpd_zero, &mpd_ctx);

    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->asks);
    while ((node = skiplist_next(iter)) != NULL) {
        order_t *order = node->value;
        mpd_add(ask_amount, ask_amount, order->left, &mpd_ctx);
    }
    skiplist_release_iterator(iter);

    iter = skiplist_get_iterator(m->bids);
    while ((node = skiplist_next(iter)) != NULL) {
        order_t *order = node->value;
        mpd_add(bid_amount, bid_amount, order->left, &mpd_ctx);
    }

    return 0;
}

sds market_status(sds reply)
{
    reply = sdscatprintf(reply, "order last ID: %"PRIu64"\n", order_id_start);
    reply = sdscatprintf(reply, "deals last ID: %"PRIu64"\n", deals_id_start);
    return reply;
}

//#ifdef CONVERSION

static size_t post_write_callback(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    sds *reply = userdata;
    *reply = sdscatlen(*reply, ptr, size * nmemb);
    return size * nmemb;
}

json_t *update_balance_main_match(json_t* request)
{
    json_t *reply  = NULL;
    json_t *error  = NULL;
    json_t *result = NULL;

    char *request_data = json_dumps(request, 0);

    CURL *curl = curl_easy_init();
    sds reply_str = sdsempty();
    log_info("update balance main begin");
    struct curl_slist *chunk = NULL;
    chunk = curl_slist_append(chunk, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
    curl_easy_setopt(curl, CURLOPT_URL, settings.mainmarket);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, post_write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &reply_str);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, (long)(1000));
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request_data);

    CURLcode ret = curl_easy_perform(curl);
    log_info("mainmarket url: %s", settings.mainmarket);
    log_info("mainmarket request: %s", json_dumps(request, 0));
    if (ret != CURLE_OK) {
        log_fatal("curl_easy_perform fail: %s", curl_easy_strerror(ret));
        goto cleanup;
    }

    reply = json_loads(reply_str, 0, NULL);
    log_info("mainmarket reply: %s", reply_str);
    if (reply == NULL)
        goto cleanup;
    error = json_object_get(reply, "error");
    if (!json_is_null(error)) {
        log_error("update balance main match fail: %s", reply_str);
        goto cleanup;
    }
    result = json_object_get(reply, "result");
    json_incref(result);

cleanup:
    free(request_data);
    sdsfree(reply_str);
    curl_easy_cleanup(curl);
    curl_slist_free_all(chunk);
    if (reply)
        json_decref(reply);

    log_info("mainmarket result: %s", json_dumps(result, 0));
    return result;
}


int market_put_conversion_maker(bool real, json_t **result, market_t *m, uint32_t user_id, mpd_t *amount, mpd_t *price)
{

    order_t *order = malloc(sizeof(order_t));
    if (order == NULL) {
        return -__LINE__;
    }
    const char* source = "source";
    const char* token = "test";
    order->id           = ++order_id_start;
    order->type         = MARKET_ORDER_TYPE_LIMIT;
    order->side         = MARKET_ORDER_SIDE_ASK;
    order->create_time  = current_timestamp();
    order->update_time  = order->create_time;
    order->market       = strdup(m->name);
    order->source       = strdup(source);
    order->user_id      = user_id;
    order->price        = mpd_new(&mpd_ctx);
    order->amount       = mpd_new(&mpd_ctx);
    order->taker_fee    = mpd_new(&mpd_ctx);
    order->maker_fee    = mpd_new(&mpd_ctx);
    order->left         = mpd_new(&mpd_ctx);
    order->freeze       = mpd_new(&mpd_ctx);
    order->deal_stock   = mpd_new(&mpd_ctx);
    order->deal_money   = mpd_new(&mpd_ctx);
    order->deal_fee     = mpd_new(&mpd_ctx);
    order->token        = strdup(token);
    order->token_rate   = mpd_new(&mpd_ctx);
    order->asset_rate   = mpd_new(&mpd_ctx);
    order->discount     = mpd_new(&mpd_ctx);
    order->deal_token   = mpd_new(&mpd_ctx);

    mpd_copy(order->price, price, &mpd_ctx);
    mpd_copy(order->amount, amount, &mpd_ctx);
    mpd_copy(order->taker_fee, mpd_zero, &mpd_ctx);
    mpd_copy(order->maker_fee, mpd_zero, &mpd_ctx);
    mpd_copy(order->left, amount, &mpd_ctx);
    mpd_copy(order->freeze, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_stock, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_money, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_fee, mpd_zero, &mpd_ctx);
    mpd_copy(order->discount, mpd_zero, &mpd_ctx);
    mpd_copy(order->token_rate, mpd_zero, &mpd_ctx);
    mpd_copy(order->asset_rate, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_token, mpd_zero, &mpd_ctx);

    int ret = 0;
    if (real)
    {
        ret = append_order_history(order);
        log_info("conversion maker:  push message");
        push_order_message(ORDER_EVENT_PUT, order, m);
        log_info("conversion maker: get order info");
        *result = get_order_info(order);
    }
    log_info("conversion maker: order put");
    ret = order_put(m, order);

    return ret;

}

static bool withdraw_conversion(uint64_t id, uint32_t user_id, const char* asset_name, const char* amount)
{
    json_t *request = json_object();
    json_t *request_params = json_array();
    json_t *detail = json_object();
    json_object_set_new(detail , "conversion_id", json_integer(id));
    json_object_set_new(detail , "action", json_integer(WITHDRAW));
    json_array_append_new(request_params, json_integer(user_id));
    json_array_append_new(request_params, json_string(asset_name));
    json_array_append_new(request_params, json_string("conversion"));
    json_array_append_new(request_params, json_integer(time(NULL)));
    json_array_append_new(request_params, json_string(amount));
    json_array_append_new(request_params, detail);
    json_object_set_new(request, "method", json_string("balance.withdraw"));
    json_object_set_new(request, "params", request_params);
    json_object_set_new(request, "id", json_integer(time(NULL)));
    json_t* reply = update_balance_main_match(request);
    json_t* status = json_object_get(reply, "status");
    if (!status || !json_is_string(status))
    {
        json_decref(request_params);
        json_decref(request);
        return false;
    }

    const char* sds_status = json_string_value(status);
    if (0 != strcmp(sds_status,"success"))
    {
        json_decref(request_params);
        json_decref(request);
        return false;
    }

    return true;
}

static bool update_conversion(uint64_t id, uint32_t user_id, const char* asset_name, const char* amount)
{
    json_t *request = json_object();
    json_t *request_params = json_array();
    json_t *detail = json_object();
    json_object_set_new(detail , "conversion_id", json_integer(id));
    json_object_set_new(detail , "action", json_integer(UPDATE));
    json_array_append_new(request_params, json_integer(user_id));
    json_array_append_new(request_params, json_string(asset_name));
    json_array_append_new(request_params, json_string("conversion"));
    json_array_append_new(request_params, json_integer(time(NULL)));
    json_array_append_new(request_params, json_string(amount));
    json_array_append_new(request_params, detail);
    json_object_set_new(request, "method", json_string("balance.update"));
    json_object_set_new(request, "params", request_params);
    json_object_set_new(request, "id", json_integer(time(NULL)));
    json_t* reply = update_balance_main_match(request);
    json_t* status = json_object_get(reply, "status");
    if (!status || !json_is_string(status))
    {
        json_decref(request_params);
        json_decref(request);
        return false;
    }

    const char* sds_status = json_string_value(status);
    if (0 != strcmp(sds_status,"success"))
    {
        json_decref(request_params);
        json_decref(request);
        return false;
    }

    return true;
}

static int conversion_backpledge(uint64_t id,uint32_t user_id_bid, const char* money_name, const char *money_amount)
{
    const char *symbol = "-";
    sds back_money = sdsempty();
    sdscpy(back_money,symbol);
    back_money = sdscat(back_money,money_amount);

    
   // printf("conversion back amount! %s : %s \n", money_amount,back_money);

    json_t *request = json_object();
    json_t *request_params = json_array();
    json_t *detail = json_object();
  
    json_object_set_new(detail , "conversion_id", json_integer(id));
    json_object_set_new(detail , "action", json_integer(BACKPLEDGE));
    json_array_append_new(request_params, json_integer(user_id_bid));
    json_array_append_new(request_params, json_string(money_name));
    json_array_append_new(request_params, json_string("conversion"));
    json_array_append_new(request_params, json_integer(time(NULL)+1));
    json_array_append_new(request_params, json_string(back_money));
    json_array_append_new(request_params, detail);
    json_object_set_new(request, "method", json_string("balance.freeze"));
    json_object_set_new(request, "params", request_params);
    json_object_set_new(request, "id", json_integer(time(NULL)));
    json_t* reply = update_balance_main_match(request);
    json_t* status = json_object_get(reply, "status");
    if (!status || !json_is_string(status))
    {
	log_info("ccccccccc");        
	json_decref(request_params);
        json_decref(request);
        return 26;
    }

    const char* sds_status = json_string_value(status);
    if (0 != strcmp(sds_status,"success"))
    {
        json_decref(request_params);
        json_decref(request);
        return 26;
    }

    return 0;

}
static int conversion_settlement(uint64_t id, uint32_t user_id_ask, uint32_t user_id_bid, const char* stock_name,
                                 const char* stock_amount, const char* money_name, const char *money_amount)
{
    const char *symbol = "-";
    sds withdraw_stock = sdsempty();
    sdscpy(withdraw_stock,symbol);
    withdraw_stock = sdscat(withdraw_stock,stock_amount);

    if( !withdraw_conversion(id, user_id_ask, stock_name, withdraw_stock))
    {
        return 20;
    }

    if( !update_conversion(id, user_id_ask, money_name, money_amount))
    {
        return 21;
    }

    sds withdraw_money = sdsempty();
    sdscpy(withdraw_money,symbol);
    withdraw_money = sdscat(withdraw_money, money_amount);
    if( !withdraw_conversion(id, user_id_bid, money_name, withdraw_money))
    {
        return 23;
    }

    if( !update_conversion(id, user_id_bid, stock_name, stock_amount))
    {
        return 22;
    }


    return 0;

}
static json_t* conversion_success(/*const char *deal_name,*/ char *deal_amount)
{
    json_t *info = json_object();
    //maybe deal_name will release
    json_object_set_new(info, "data", json_string(deal_amount));
    return info;
}

static json_t* execute_taker_order(bool real, market_t *m, order_t *maker_ask, uint32_t user_id,
                                char *amount, char *stock_volume, char *money_amount)
{
    order_t *taker_bid = malloc(sizeof(order_t));
    if (taker_bid == NULL) {
        return false;
    }
    const char* source = "source";
    const char* token = "test";

    taker_bid->id           = ++order_id_start;
    taker_bid->type         = MARKET_ORDER_TYPE_LIMIT;
    taker_bid->side         = MARKET_ORDER_SIDE_BID;
    taker_bid->create_time  = current_timestamp();
    taker_bid->update_time  = taker_bid->create_time;
    taker_bid->market       = strdup(maker_ask->market);
    taker_bid->source       = strdup(source);
    taker_bid->user_id      = user_id;
    taker_bid->price        = mpd_new(&mpd_ctx);
    taker_bid->amount       = mpd_new(&mpd_ctx);
    taker_bid->taker_fee    = mpd_new(&mpd_ctx);
    taker_bid->maker_fee    = mpd_new(&mpd_ctx);
    taker_bid->left         = mpd_new(&mpd_ctx);
    taker_bid->freeze       = mpd_new(&mpd_ctx);
    taker_bid->deal_stock   = mpd_new(&mpd_ctx);
    taker_bid->deal_money   = mpd_new(&mpd_ctx);
    taker_bid->deal_fee     = mpd_new(&mpd_ctx);
    taker_bid->token        = strdup(token);
    taker_bid->token_rate   = mpd_new(&mpd_ctx);
    taker_bid->asset_rate   = mpd_new(&mpd_ctx);
    taker_bid->discount     = mpd_new(&mpd_ctx);
    taker_bid->deal_token   = mpd_new(&mpd_ctx);

    mpd_copy(taker_bid->price,maker_ask->price, &mpd_ctx);
    mpd_copy(taker_bid->amount,decimal(amount,m->stock_prec), &mpd_ctx);
    mpd_copy(taker_bid->taker_fee, mpd_zero, &mpd_ctx);
    mpd_copy(taker_bid->maker_fee, mpd_zero, &mpd_ctx);
    mpd_copy(taker_bid->left, mpd_zero, &mpd_ctx);
    mpd_copy(taker_bid->freeze, mpd_zero, &mpd_ctx);
    mpd_copy(taker_bid->deal_stock, decimal(stock_volume, m->stock_prec), &mpd_ctx);
    mpd_copy(taker_bid->deal_money, decimal(money_amount, m->money_prec), &mpd_ctx);
    mpd_copy(taker_bid->deal_fee, mpd_zero, &mpd_ctx);
    mpd_copy(taker_bid->discount, mpd_zero, &mpd_ctx);
    mpd_copy(taker_bid->token_rate, mpd_zero, &mpd_ctx);
    mpd_copy(taker_bid->asset_rate, mpd_zero, &mpd_ctx);
    mpd_copy(taker_bid->deal_token, mpd_zero, &mpd_ctx);

    if (real)
    {
        append_order_history(taker_bid);
        push_order_message(ORDER_EVENT_FINISH, taker_bid, m);
        uint64_t deal_id = ++deals_id_start;
        mpd_t *ask_fee = mpd_new(&mpd_ctx);
        mpd_t *bid_fee = mpd_new(&mpd_ctx);
        mpd_t *ask_deal_token = mpd_new(&mpd_ctx);
        mpd_t *bid_deal_token = mpd_new(&mpd_ctx);

        mpd_copy(ask_fee, mpd_zero, &mpd_ctx);
        mpd_copy(bid_fee, mpd_zero, &mpd_ctx);
        mpd_copy(ask_deal_token, mpd_zero, &mpd_ctx);
        mpd_copy(bid_deal_token, mpd_zero, &mpd_ctx);

        append_order_deal_history(taker_bid->update_time, deal_id, taker_bid, MARKET_ROLE_TAKER,
                                  maker_ask,MARKET_ROLE_MAKER, maker_ask->price, maker_ask->amount,
                                  decimal(money_amount, m->money_prec), ask_fee, bid_fee, ask_deal_token, bid_deal_token);

        push_deal_message(taker_bid->update_time, m->name, taker_bid, maker_ask,
                          taker_bid->price, decimal(money_amount, m->money_prec), ask_fee, bid_fee, MARKET_ORDER_SIDE_BID, deal_id,
                          m->stock, m->money, ask_deal_token, bid_deal_token);

        mpd_del(ask_fee);
        mpd_del(bid_fee);
        mpd_del(ask_deal_token);
        mpd_del(bid_deal_token);

        return  get_order_info(taker_bid);
    }
    log_info("conversion maker: order put");

    return NULL;
}

int market_put_conversion_taker(bool real, json_t **result, market_t *m, uint32_t user_id, order_t *order, mpd_t *volume,
                                const char *stock_name, const char *money_name)
{
    mpd_t *left_money = mpd_new(&mpd_ctx);
    mpd_mul(left_money, order->price, order->left, &mpd_ctx);

    mpd_t *taker_money = mpd_new(&mpd_ctx);
    mpd_mul(taker_money, order->price, volume, &mpd_ctx);
    char *deal_volume = mpd_to_sci(volume,0);
    int ret = 0;
    if ( mpd_cmp(left_money, taker_money, &mpd_ctx) <= 0 )
    {
	log_info("right");
        char *stock_volume = mpd_to_sci(order->left, 0);
        char *money_amount = mpd_to_sci(left_money, 0);
        if ( real )
        {
	   
            // ret = conversion_settlement(order->user_id, user_id, stock_name, stock_volume, money_name, money_amount);
            ret = conversion_settlement(order->id, order->user_id, user_id, stock_name, stock_volume, money_name, money_amount);
	    mpd_t *backpledge = mpd_new(&mpd_ctx);
	    mpd_sub(backpledge, taker_money, left_money, &mpd_ctx);
	    char *back_amount = mpd_to_sci(backpledge, 0);
	    
            if( 0 == ret)
            {
                *result = execute_taker_order(real, m, order,user_id, deal_volume, stock_volume, money_amount);
                ret = append_order_history(order);
                //*result = conversion_success(stock_volume);
                push_order_message(ORDER_EVENT_FINISH, order, m);
		log_info("back amount");
		mpd_t * min_amount_update = mpd_new(&mpd_ctx);
		mpd_set_string(min_amount_update, "0.00000001", &mpd_ctx);
		if (mpd_cmp(backpledge, min_amount_update, &mpd_ctx) >= 0)
		{
	             ret = conversion_backpledge(order->id, user_id, money_name, back_amount);
		}
		mpd_del(backpledge);
		mpd_del(min_amount_update);

		//ret = conversion_backpledge(order->id, user_id, money_name, back_amount);
            }
        }
        order_finish(real,m,order);
    }
    else
    {

	log_info("left");
        mpd_t *left_stock = mpd_new(&mpd_ctx);
        mpd_sub(left_stock, order->left, volume, &mpd_ctx);

        char *stock_amount = mpd_to_sci(volume,0);
        char *money_amount = mpd_to_sci(taker_money, 0);

        mpd_copy(order->left, left_stock, &mpd_ctx);
        if( real )
        {

            // ret = conversion_settlement(order->user_id, user_id, stock_name, stock_amount, money_name, money_amount);
            ret = conversion_settlement(order->id, order->user_id, user_id, stock_name, stock_amount, money_name, money_amount);
            if( 0 == ret)
            {
                *result =  execute_taker_order(real, m, order, user_id, deal_volume, stock_amount, money_amount);

                ret = append_order_history(order);
               // *result = conversion_success(stock_amount);
                push_order_message(ORDER_EVENT_UPDATE, order, m);
            }
        }

        mpd_del(left_stock);
    }

    mpd_del(left_money);
    mpd_del(taker_money);
    return ret;
}
//#endif



