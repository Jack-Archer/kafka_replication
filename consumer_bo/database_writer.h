#pragma once

#include "libpq-fe.h"
#include <memory>
#include <atomic>

#include "consumer_librdkafka.h"

class DatabaseWriter
{
    public:
        DatabaseWriter();
        ~DatabaseWriter();

        bool connect();
        bool handlerIncomingData(ConsumerMessage &data_to_save);
        std::string getColumnType(const Json::Value& value);
        std::string buildCreateTableSql(const std::string &table_name, const Json::Value &data);
        bool tableExists(const std::string &table_name);
        bool createTableIfNotExists(const std::string &table_name, const Json::Value &data);
        std::string escapeString(const std::string& str);

        bool insertRow(const std::string& table_name, const Json::Value& data);
        bool updateRow(const std::string& table_name, const Json::Value& before, const Json::Value& after,const std::string& primary_key);
        bool deleteRow(const std::string& table_name, const Json::Value& before, const std::string& primary_key);

        const bool status() const;

    private:
        ConnInfo connifo_;
        PGconn *conn_;
        std::atomic<bool> running_;
};