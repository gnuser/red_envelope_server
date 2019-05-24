/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/01, create
 */

# ifndef _ME_OPERLOG_H_
# define _ME_OPERLOG_H_

# include "me_config.h"

extern uint64_t operlog_id_start;

int init_operlog(void);
int fini_operlog(void);

int append_operlog(const char *method, json_t *params);
// envelope
int append_operlog_time(const char *method, json_t *params, double time);

bool is_operlog_block(void);
sds operlog_status(sds reply);

# endif

