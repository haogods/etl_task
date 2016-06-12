#pragma once
#include <mongoc.h>


class CMongo
{
private:
        char m_db_name[256];
        mongoc_client_pool_t *m_pool;
        mongoc_client_t * m_client;
		mongoc_collection_t * m_collection;
		mongoc_cursor_t * m_cursor;
		mongoc_write_concern_t *m_waitdef, *m_nowait;


		char * m_rstr;
protected:
        bool update_bson(const bson_t* bkey, const bson_t* bdoc, bool upsert, bool many, bool wait);
        bool update_json(const char * jkey, const char * jdoc, bool upsert, bool many, bool wait);


public:
		bson_error_t m_error;
		CMongo();
		~CMongo();
		char * save_str(char *str);
		
		const bson_error_t * get_err() const
        {
                return &m_error;
        }

		bool init(const char *uristr, const char *db_name, const char *collection_name);

		bool update_one(const char* bkey, const char* bdoc, bool upsert)
  		{
			return update_json(bkey, bdoc, upsert, false, true);
		}
		
		bool update_one_no_wait(const char* bkey, const char* bdoc, bool upsert)
		{
			return update_json(bkey, bdoc, upsert, false, false);
		}
		
		bool update_many(const char* bkey, const char* bdoc, bool upsert)
		{
			return update_json(bkey, bdoc, upsert, true, true);
		}
        
 		bool update_many_no_wait(const char* bkey, const char* bdoc, bool upsert)
		{
			return update_json(bkey, bdoc, upsert, true, false);
		}

        char* find_one(const char *key);

        bool delete_one(const char* doc);

};
