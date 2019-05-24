/*
 * Description: 
 *     History: yang@haipo.me, 2017/03/16, create
 */

# ifndef _ME_MARKET_H_
# define _ME_MARKET_H_

# include "me_config.h"

extern uint64_t order_id_start;
extern uint64_t deals_id_start;

typedef struct order_t {
    uint64_t        id;
    uint32_t        type;
    uint32_t        side;
    uint32_t        user_id;
    double          create_time;
    char            *market;
// envelope
    char            *asset;       
    double          supply;
    double          leave;
    uint32_t        share;
    uint32_t        expire_time;
    uint32_t        count;
    char            *uids;
    char            *amounts;
    char            *times;
} order_t;

typedef struct market_t {
    char            *name;
    char            *stock;
    char            *money;

    int             stock_prec;
    int             money_prec;
    int             fee_prec;
    mpd_t           *min_amount;

    dict_t          *orders;
    dict_t          *users;

    skiplist_t      *asks;
    skiplist_t      *bids;
} market_t;

market_t *market_create(struct market *conf);

int market_put_order(market_t *m, order_t *order);

json_t *get_order_info(order_t *order, int pos);
order_t *market_get_order(market_t *m, uint64_t id);
skiplist_t *market_get_order_list(market_t *m, uint32_t user_id);

# endif

