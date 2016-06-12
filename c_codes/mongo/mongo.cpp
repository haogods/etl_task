//#include "stdafx.h"
#include <decimal/decimal>
typedef std::decimal::decimal32  _Decimal32;
typedef std::decimal::decimal64  _Decimal64;
typedef std::decimal::decimal128 _Decimal128;
#include <mongoc.h>
#include "mongo.h"


CMongo::CMongo()
{
        m_client = 0;
        m_collection = 0;
        m_db_name[0] = 0;
        m_cursor = 0;
        m_rstr = 0;
        m_waitdef = mongoc_write_concern_new();
        mongoc_write_concern_set_w(m_waitdef, MONGOC_WRITE_CONCERN_W_DEFAULT);
        m_nowait = mongoc_write_concern_new();
        mongoc_write_concern_set_w(m_nowait, MONGOC_WRITE_CONCERN_W_UNACKNOWLEDGED);
        memset(&m_error, 0, sizeof(m_error));
}

CMongo::~CMongo()
{
        save_str(NULL);
        mongoc_write_concern_destroy(m_waitdef);
        mongoc_write_concern_destroy(m_nowait);
        if (m_cursor)
        {
                mongoc_cursor_destroy(m_cursor);
                m_cursor = 0;
        }

        if (m_collection)
        {
                mongoc_collection_destroy(m_collection);
        }
        if (m_client)
        {
                mongoc_client_destroy(m_client);
        }

}


char* CMongo::save_str(char * str)
{
        if(m_rstr)
        {
            bson_free(m_rstr);
        }
        m_rstr = str;
        return m_rstr;
}
bool CMongo::delete_one(const char* doc){

    if (!doc) return false;

    bson_error_t error;
    bson_t *specific;

    bool rs = true;

    specific = bson_new_from_json((const uint8_t *)doc, -1, &error);

    if (!mongoc_collection_remove (m_collection, MONGOC_REMOVE_SINGLE_REMOVE, specific, NULL, &error)) {
        fprintf (stderr, "Delete failed: %s\n", error.message);
        rs = false;
    }

    bson_destroy(specific);

    return rs;

}


char* CMongo::find_one(const char *key){


    if (!key) return NULL;

    const bson_t *doc;
    char *str=NULL;
    bson_t *query;
    mongoc_cursor_t *cursor;

    query = bson_new_from_json((const uint8_t *)key, -1, &m_error);

    cursor = mongoc_collection_find (m_collection, MONGOC_QUERY_NONE, 0, 0, 0, query, NULL, NULL);

    while (mongoc_cursor_next (cursor, &doc)) {
          str =  bson_as_json (doc, NULL);
          break;
    }

    mongoc_cursor_destroy (cursor);

    return str;

}

bool CMongo::init(const char * uristr, const char * dbname, const char * collection_name)
{           
        printf("==========================================\n");
        printf("url: %s\n, dbname: %s\n, coll:%s\n", uristr, dbname, collection_name);
        m_db_name[0] = 0;
        m_client = mongoc_client_new(uristr);
        if (m_client)
        {
                if(!dbname)
                {
                    dbname = mongoc_uri_get_database(mongoc_client_get_uri(m_client));
                }
                if (!dbname)
                {
                    dbname = "admin";
                }
                strncpy(m_db_name, dbname, sizeof(m_db_name)-1);
                m_db_name[sizeof(m_db_name)-1] = 0;
                m_collection = mongoc_client_get_collection(m_client, dbname, collection_name);
                return true;

        }
        return false;
}

bool CMongo::update_json(const char * jkey, const char * jdoc, bool upsert, bool many, bool wait)
{
        if (!jkey || !jdoc) return false;
        bson_t *bkey = bson_new_from_json((const uint8_t *)jkey, -1, &m_error);
        if (!bkey)
        {
            return false;
        }

        bson_t * bdoc = bson_new_from_json((const uint8_t*)jdoc, -1, &m_error);
        if (!bdoc)
        {
            bson_destroy(bkey);
            return false;
        }

        bool r = update_bson(bkey, bdoc, upsert, many, wait);
        bson_destroy(bkey);
        bson_destroy(bdoc);

        return r;
}

bool CMongo::update_bson(const bson_t* bkey, const bson_t* bdoc, bool upsert, bool many, bool wait)
{
        mongoc_update_flags_t fl = (mongoc_update_flags_t) MONGOC_UPDATE_NO_VALIDATE;
        if (upsert) fl = (mongoc_update_flags_t) (fl|MONGOC_UPDATE_UPSERT);

        if (many) fl = (mongoc_update_flags_t) (fl|MONGOC_UPDATE_MULTI_UPDATE);
       // mongoc_collection_t * collection = mongoc_collection_copy(m_collection);
        return mongoc_collection_update(m_collection, fl, bkey, bdoc, wait?m_waitdef:m_nowait, &m_error);
}


extern "C"
{
        intptr_t new_CMongo(){return (intptr_t) new CMongo();}
        void delete_CMongo(intptr_t _this)
        {
                CMongo *cmg = (CMongo*) _this;
                delete cmg;

        }

        char * save_str(intptr_t _this, char *str)
        {
                printf("%s", str);
                CMongo *cmg = (CMongo*) _this;
                return cmg -> save_str(str);
        }

        bool init(intptr_t _this, const char * uristr, const char * dbname, const char * collection_name)
        {
                CMongo *cmg = (CMongo*) _this;
                return cmg -> init(uristr, dbname, collection_name);
        }

        bool update_one(intptr_t _this, const char * jkey, const char * jdoc, bool upsert)
        {
                CMongo *cmg = (CMongo*) _this;
                return cmg -> update_one(jkey, jdoc, upsert);
        }

        bool update_one_no_wait(intptr_t _this, const char *jkey, const char * jdoc, bool upsert)
        {
                CMongo *cmg = (CMongo*) _this;
                return cmg -> update_one_no_wait(jkey, jdoc, upsert);
        }

        bool update_many(intptr_t _this, const char * jkey, const char * jdoc, bool upsert)
        {
                CMongo *cmg = (CMongo*) _this;
                return cmg -> update_many(jkey, jdoc, upsert);
        }

        bool update_many_no_wait(intptr_t _this, const char *jkey, const char * jdoc, bool upsert)
        {
                CMongo *cmg = (CMongo*) _this;
                return cmg -> update_many_no_wait(jkey, jdoc, upsert);
        }

        char* find_one(intptr_t _this, const char *key)
        {

                CMongo *cmg = (CMongo*) _this;
                return cmg -> find_one(key);
        }


        bool delete_one(intptr_t _this, const char* key)
        {

                CMongo *cmg = (CMongo*) _this;
                return cmg -> delete_one(key);

        }
}


//gcc -fPIC mongo.cpp -L/usr/local/lib/ -lmongoc-1.0 -I /usr/local/include/libmongoc-1.0/ -I /usr/local/include/libbson-1.0/ -shared -o libmongo.so -lstdc++