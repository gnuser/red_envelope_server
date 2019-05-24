/*
 * Description: user balance manage
 *     History: yang@haipo.me, 2017/03/15, create
 */

# ifndef _ME_BALANCE_H_
# define _ME_BALANCE_H_

# include "me_config.h"

# define BALANCE_TYPE_AVAILABLE 1
# define BALANCE_TYPE_FREEZE    2
# define BALANCE_TYPE_PLEDGE	3

extern dict_t *dict_balance;

struct balance_key {
    uint32_t    user_id;
    uint32_t    type;
    char        asset[ASSET_NAME_MAX_LEN + 1];
};

int init_balance(void);

bool asset_exist(const char *asset);
int asset_prec(const char *asset);
int asset_prec_show(const char *asset);

mpd_t *balance_get(uint32_t user_id, uint32_t type, const char *asset);
void   balance_del(uint32_t user_id, uint32_t type, const char *asset);
mpd_t *balance_set(uint32_t user_id, uint32_t type, const char *asset, mpd_t *amount);

# endif

