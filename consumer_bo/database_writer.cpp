#include "database_writer.h"

DatabaseWriter::DatabaseWriter() : conn_(nullptr), running_(false)
{
    connifo_ = ConnInfo();
    running_ = connect();

    if(!running_)
    {
        throw std::runtime_error ("Failed start database writer");
    }
}

DatabaseWriter::~DatabaseWriter()
{

}

const bool DatabaseWriter::status() const
{
    return running_;
}

bool DatabaseWriter::connect()
{
    conn_ = PQconnectdb(connifo_.concateToString().c_str());
    if(PQstatus(conn_) != CONNECTION_OK)
    {
        std::cerr << "Connection failed: " << PQerrorMessage(conn_) << std::endl;
        PQfinish(conn_);
        conn_ = nullptr;
        return false;
    }

    std::cout << "Connected to PostgreSQL" << std::endl;
    return true;
}



bool DatabaseWriter::tableExists(const std::string &table_name)
{
    std::string query =  "SELECT 1 FROM information_schema.tables WHERE table_name = '" + table_name + "'";

    PGresult *res = PQexec(conn_, query.c_str());
    bool exists = (PQntuples(res) > 0);
    PQclear(res);
    return exists;
}


bool DatabaseWriter::createTableIfNotExists(const std::string &table_name, const Json::Value &data)
{
    if(tableExists(table_name))
    {
        std::cout << "Table " << table_name << " already exists" << std::endl;
        return true;
    }

    std::string sql_query = buildCreateTableSql(table_name, data);
    PGresult *res = PQexec(conn_, sql_query.c_str());
    std::cout << "Creating table: " << sql_query << std::endl;

    if(PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        std::cerr << "Failed to create table: " << PQerrorMessage(conn_) << std::endl;
        PQclear(res);
        return false;
    }

    PQclear(res);
    std::cout << "Table " << table_name << " created" << std::endl;
    return true;
}


std::string DatabaseWriter::getColumnType(const Json::Value& value) {
    if (value.isInt()) return "INTEGER";
    if (value.isInt64()) return "BIGINT";
    if (value.isDouble()) return "DOUBLE PRECISION";
    if (value.isBool()) return "BOOLEAN";
    if (value.isString()) return "TEXT";
    if (value.isNull()) return "TEXT";
    return "TEXT";
}

std::string DatabaseWriter::escapeString(const std::string& str)
{
    size_t len = str.size();

    char *escaped = new char(len * 2 + 1);
    PQescapeStringConn(conn_, escaped, str.c_str(), len, nullptr);
    std::string result(escaped);
    delete[] escaped;
    return result;
}

std::string DatabaseWriter::buildCreateTableSql(const std::string &table_name, const Json::Value &data)
{
    std::stringstream sql;
    sql << "CREATE TABLE IF NOT EXISTS " << table_name << " (";

    auto members = data.getMemberNames();
    bool first = true;
    
    for(const auto key : members)
    {
        if(!first) sql << ", ";
        first = false;

        std::string type = getColumnType(data[key]);
        sql << "\"" << key << "\" " << type;
    }
    sql << ")";
    return sql.str();
}

bool DatabaseWriter::insertRow(const std::string& table_name, const Json::Value& data)
{
    std::stringstream columns, values;

    auto members = data.getMemberNames();
    bool first = true;

    for(const auto &key : members)
    {
        if (!first) {
            columns << ", ";
            values << ", ";
        }
        first = false;

        columns << "\"" << key << "\"";

        if(data[key].isNull())
        {
            values << "NULL";
        } else if(data[key].isString())
            {
                values << "'" << escapeString(data[key].asString()) << "'";
            } else if(data[key].isInt())
                {
                    values << data[key].asInt();
                } else if(data[key].isInt64())
                    {
                        values << data[key].asInt64();
                    } else if(data[key].isDouble())
                        {
                            values << data[key].asDouble();
                        } else if(data[key].isBool())
                            {
                                values << data[key].asBool() ? "TRUE" : "FALSE";
                            } else
                                {
                                    values << "'" << escapeString(data[key].toStyledString()) << "'";
                                }
    }

    std::string sql = "INSERT INTO " + table_name + " (" + columns.str() + ") VALUES (" + values.str() + ")";

    PGresult *res = PQexec(conn_, sql.c_str());

    if(PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        std::cerr << "INSERT failed: " << PQerrorMessage(conn_) << std::endl;
        PQclear(res);
        return false;
    }

    PQclear(res);
    std::cout << "Inserted into " << table_name << std::endl;
    return true;
}

bool DatabaseWriter::updateRow(const std::string& table_name, const Json::Value& before, const Json::Value& after, const std::string& primary_key)
{
    std::string key_column, key_value;
    auto members_before = before.getMemberNames();

    if(!primary_key.empty())
    {
        key_column = primary_key;
    } else {
        key_column = members_before[0];
    }

    key_value = "'" + before[key_column].asString() + "'";
    //------------------------------------------------------primary done, next to after
    std::stringstream set_clause;
    bool first = true;
    auto members_after = after.getMemberNames();

    for(const auto &key : members_after)
    {
        if(!first) set_clause << ", ";
        first = false;

        set_clause << "\"" << key << "\"";

        if (after[key].isNull()) {
            set_clause << "NULL";
        } else if (after[key].isString()) {
            set_clause << "'" << escapeString(after[key].asString()) << "'";
        } else if (after[key].isInt()) {
            set_clause << after[key].asInt();
        } else if (after[key].isInt64()) {
            set_clause << after[key].asInt64();
        } else if (after[key].isDouble()) {
            set_clause << after[key].asDouble();
        } else if (after[key].isBool()) {
            set_clause << (after[key].asBool() ? "TRUE" : "FALSE");
        } else {
            set_clause << "'" << escapeString(after[key].toStyledString()) << "'";
        }
    }

    std::string sql = "UPDATE " + table_name + " SET " + set_clause.str() + " WHERE \"" + key_column + "\" = " + key_value;

    PGresult *res = PQexec(conn_, sql.c_str());
    if(PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        PQclear(res);
        std::cerr << "UPDATE failed: " << PQerrorMessage(conn_) << std::endl;
        return false;
    }

    PQclear(res);
    std::cout << "Updated " << table_name << std::endl;
    return true;
}

bool DatabaseWriter::deleteRow(const std::string& table_name, const Json::Value& before, const std::string& primary_key)
{
    std::string key_column, key_value;

    auto members = before.getMemberNames();

    if(!primary_key.empty())
    {
        key_column = primary_key;
    } else
        {
            key_column = members[0];
        }
    
    key_value = "'" + before[key_column].asString() + "'";
    
    std::string sql = "DELETE FROM " + table_name + " WHERE \"" + key_column + "\" = " + key_value;

    PGresult *res = PQexec(conn_, sql.c_str());
    if(PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        PQclear(res);
        std::cerr << "DELETE failed: " << PQerrorMessage(conn_) << std::endl;
        return false;
    }

    PQclear(res);
    std::cout << "Deleted from " << table_name << std::endl;
    return true;
}

bool DatabaseWriter::handlerIncomingData(ConsumerMessage &data_to_save)
{
    bool result = false;
    std::cout << "Save data : " << data_to_save.op << "  in table  " << data_to_save.table << std::endl;

    if(data_to_save.op == "INSERT")
    {
        std::cout << "Incoming INSERT\n";
        result = createTableIfNotExists(data_to_save.table, data_to_save.data);
        result = insertRow(data_to_save.table, data_to_save.data);
    } else if(data_to_save.op == "UPDATE")
        {
            result = updateRow(data_to_save.table, data_to_save.before, data_to_save.after, data_to_save.primary_key);
            std::cout << "Incoming UPDATE\n";
        } else  if(data_to_save.op == "DELETE")
            {
                result = deleteRow(data_to_save.table, data_to_save.before, data_to_save.primary_key);
                std::cout << "Incoming DELETE\n";
            } else if(data_to_save.op == "TRUNCATE")
                {
                    std::cout << "Incoming TRUNCATE\n";
                } else{
                    std::cout << "Unknown operation\n";
                }

    return result;
}