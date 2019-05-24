/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/04, create
 */

# ifndef _ME_PERSIST_H_
# define _ME_PERSIST_H_

# include <time.h>
# include "me_market.h"

int init_persist(void);

int init_from_db(market_t *market);
int dump_to_db(time_t timestamp);
int make_slice(time_t timestamp);
int clear_slice(time_t timestamp);

int init_asset_from_db(MYSQL *conn);
int init_market_from_db(MYSQL *conn);
int init_asset_and_market(bool market);

int asset_update(const char *asset);

# endif

