/*
 * Description: 
 *     History: yang@haipo.me, 2017/03/18, create
 */

# include "me_config.h"
# include "me_update.h"
# include "me_balance.h"
# include "me_history.h"

static dict_t *dict_update;
static nw_timer timer;

struct update_key {
    uint32_t    user_id;
    char        asset[ASSET_NAME_MAX_LEN + 1];
    char        business[BUSINESS_NAME_MAX_LEN + 1];
    uint64_t    business_id;
};

struct update_val {
    double      create_time;
};

static uint32_t update_dict_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct update_key));
}

static int update_dict_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct update_key));
}

static void *update_dict_key_dup(const void *key)
{
    struct update_key *obj = malloc(sizeof(struct update_key));
    memcpy(obj, key, sizeof(struct update_key));
    return obj;
}

static void update_dict_key_free(void *key)
{
    free(key);
}

static void *update_dict_val_dup(const void *val)
{
    struct update_val*obj = malloc(sizeof(struct update_val));
    memcpy(obj, val, sizeof(struct update_val));
    return obj;
}

static void update_dict_val_free(void *val)
{
    free(val);
}

static void on_timer(nw_timer *t, void *privdata)
{
    double now = current_timestamp();
    dict_iterator *iter = dict_get_iterator(dict_update);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        struct update_val *val = entry->val;
        if (val->create_time < (now - 86400)) {
            dict_delete(dict_update, entry->key);
        }
    }
    dict_release_iterator(iter);
}

int init_update(void)
{
    dict_types type;
    memset(&type, 0, sizeof(type));
    type.hash_function  = update_dict_hash_function;
    type.key_compare    = update_dict_key_compare;
    type.key_dup        = update_dict_key_dup;
    type.key_destructor = update_dict_key_free;
    type.val_dup        = update_dict_val_dup;
    type.val_destructor = update_dict_val_free;

    dict_update = dict_create(&type, 64);
    if (dict_update == NULL)
        return -__LINE__;

    nw_timer_set(&timer, 60, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}


