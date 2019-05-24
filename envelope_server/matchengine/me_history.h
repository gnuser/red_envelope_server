/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/06, create
 */

# ifndef _ME_HISTORY_H_
# define _ME_HISTORY_H_

# include "me_market.h"

int init_history(void);
int fini_history(void);

bool is_history_block(void);
sds history_status(sds reply);

// envelope
int append_user_envelope_history(double time, uint32_t user_id, const char *asset, uint64_t envelope_id, uint32_t role, double amount);
int append_envelope_detail(double time, uint64_t envelope_id, uint32_t user_id, const char *asset, uint32_t type, 
                           const char *supply, uint32_t share, uint32_t expire_time);

# endif

