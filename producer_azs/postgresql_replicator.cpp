#include "postgresql_replicator.h"


#include <iostream>
#include <thread>
#include <arpa/inet.h>
#include <mutex>


PostgresqlReplicator::PostgresqlReplicator() : conn_(nullptr), repl_conn_(nullptr), replicating_(false)
{
    parser_ = pgoutput::PgOutputParser();
    stats_ = Stats();
    stats_.timestamp = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    conninfo_ = ConnInfo();
    config_manager_ = ConfigManager(PATH_TO_CONFIG);

    if(connect())
    {
        if(initKafka(conninfo_.brokers_names, conninfo_.topic))
        {
            std::cout << "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< Продюсер Кафка запущен >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" << std::endl;
            if(startReplicationAll())
            {
                std::cout << "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< Начинаем репликацию >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" << std::endl;
            } 
        } else {
            throw  std::runtime_error("Failed start kafka producer");
        }    
    } else {       
        stopReplication();
        throw  std::runtime_error("Failed start replication");
    }
}


PostgresqlReplicator::~PostgresqlReplicator()
{
    stopReplication();
    if(conn_){PQfinish(conn_);}
    if(repl_conn_){PQfinish(repl_conn_);}
}

void PostgresqlReplicator::stopReplication()
{

}

bool PostgresqlReplicator::connect()
{
    conn_ = PQconnectdb(conninfo_.concateToString().c_str());

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

bool PostgresqlReplicator::createReplicationSlot()
{
    std::string query =  "SELECT * FROM pg_create_logical_replication_slot('" + conninfo_.slot_name + "', '" + conninfo_.output_plugin + "')";

    PGresult *res = PQexec(conn_, query.c_str());
    if(PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        std::string err = PQerrorMessage(conn_);
        PQclear(res);
        if(err.find("already exists") != std::string::npos)
        {
            std::cout << "Slot already exists, reusing..." << std::endl;
            return true;
        }
                
        std::cerr << "Failed to create slot: " << err << std::endl;
        return false;
    }

    PQclear(res);
    std::cout << "Replication slot created: " << conninfo_.slot_name << std::endl;
    return true;
}



bool PostgresqlReplicator::createPublicationAllTables() //исправить проверку публикации и ее запуск или создание если она существует 
{
    std::string check_query = "SELECT 1 FROM  pg_publication WHERE pubname = '" + conninfo_.publication_name + "'";
    PGresult *res_test = PQexec(conn_, check_query.c_str());
    bool exists = (PQntuples(res_test) > 0);
    

    // std::string create_query = "CREATE PUBLICATION " + conninfo_.publication_name + " FOR ALL TABLES";
    
    if(!exists)
    {
        std::cout << "Create new publication " << conninfo_.publication_name << std::endl;
        std::string create_query = "CREATE PUBLICATION " + conninfo_.publication_name + " FOR ALL TABLES";
        PGresult *res = PQexec(conn_, create_query.c_str());
        if(PQresultStatus(res) != PGRES_COMMAND_OK)
        {
            std::cerr << "Failed to create publication: " << PQerrorMessage(conn_) << std::endl;
            PQclear(res);
            return false;
        }
    } else {
        std::cout << "Failed to create publication: Публикация " << conninfo_.publication_name << " уже существует" << std::endl;
    }

    PQclear(res_test);
    return true;

}



bool PostgresqlReplicator::startReplicationAll()
{
    if(!conn_)
    {
        std::cerr << "Connetcting to database failed, check the connection" << std::endl;
        return false;
    }

    createReplicationSlot();
    createPublicationAllTables();

    std::string repl_conninfo = conninfo_.concateToString() + " replication=database";
    repl_conn_ = PQconnectdb(repl_conninfo.c_str());
    
    std::cout << "CONN = " <<repl_conninfo.c_str() << std::endl;
    if(PQstatus(repl_conn_) != CONNECTION_OK)
    {
        std::cerr << "Replication connection failed: " << PQerrorMessage(repl_conn_) << std::endl;
        return false;
    }
    //-------------------------------------reading from config----------------------------------------------------------
    std::string lsn_rest_full = config_manager_.getValueByKeyFromConfig("RESTART_LSN");
    config_manager_.parseLSN(lsn_rest_full,stats_.restart_lsn_high, stats_.restart_lsn_low);
    std::cout << "Read from config. LSN: high = " << std::to_string(stats_.restart_lsn_high) << " low = " << std::to_string(stats_.restart_lsn_low) << std::endl;
    //-----------------------------------end reading from config---------------------------------------------------------
    
    std::string query = "START_REPLICATION SLOT " + conninfo_.slot_name + 
                    " LOGICAL " + std::to_string(stats_.restart_lsn_high) + "/" + std::to_string(stats_.restart_lsn_low) + " (proto_version '" + conninfo_.proto_version + "', publication_names '\"" + 
                    conninfo_.publication_name + "\"')"; //нужно изменить 0\0 на последний прочитаный и отправленый лсн

    PGresult *res = PQexec(repl_conn_, query.c_str());
    if (PQresultStatus(res) != PGRES_COPY_BOTH) {
    std::cerr << "START_REPLICATION failed: " << PQerrorMessage(repl_conn_) << std::endl;
    PQclear(res);
    return false;
    }

    PQclear(res);
    replicating_ = true;
    std::cout << "Replication started successfully" << std::endl;
    
    return true;
}


void PostgresqlReplicator::startReadReplicationMessages()
{
    if(!replicating_ || !repl_conn_){
        std::cerr << "Failed to read replication message" << std::endl;
        throw  std::runtime_error("Failed to read replication message");
    }

    char* buffer = nullptr;
    int bytes_read;

    while(replicating_)
    {
        bytes_read = PQgetCopyData(repl_conn_, &buffer, 0);

        if(bytes_read == -1)
        {
            sendStandbyStatusUpdate();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }
        if(bytes_read == -2)
        {
            throw std::runtime_error("Error reading replication data");
            break;
        }
        if(bytes_read > 0)
        {
                try {
                    // std::cout << "Data recieved from PG   =   " << bytes_read << std::endl;
                    processReplicationMessage(buffer, bytes_read); //at begin parse raw and after processReplicationMessage    
                } catch (const std::exception& e) {
                    std::cerr << "Error processing message: " << e.what() << std::endl;
                }
                
                PQfreemem(buffer);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}


void PostgresqlReplicator::processReplicationMessage(const char* buff, int bytes_read)
{
    const uint8_t *safe_data = reinterpret_cast<const uint8_t*>(buff);
    std::vector<uint8_t> message(safe_data, safe_data + bytes_read);
    parsePacket(message);
    
    // auto parsed_msg = parser_.parse_message(message);

    // if(parsed_msg)
    // {
    //     std::cerr << "DONE INPUT" << std::endl;
    // }
}


void PostgresqlReplicator::parsePacket(const std::vector<uint8_t> raw_message)
{
    
    if(raw_message.empty())
    {
        return;
    }

    char msg_type = static_cast<char>(raw_message[0]);

    // std::cout << "MSG TYPE HEAD = " << msg_type << std::endl;
    // std::cout << "SIZE OF VECTOR INCOMING = " << raw_message.size() << std::endl;
    // std::vector<uint8_t> clean_data(raw_message.begin() + 24, raw_message.end());
    // std::cout << "MSG TYPE HEAD = " << static_cast<char>(raw_message[25]) << std::endl;

    // std::shared_ptr<ReplicationMessage> message;


    try
    {
        switch (msg_type)
        {
        case 'w':
            {
                if(raw_message.size() < 25)
                {
                    throw std::runtime_error("WAL message too short");
                }

                // std::cout << "WAL packet size: " << raw_message.size() << std::endl;
                // std::cout << "First 32 bytes: ";
                // for (int i = 0; i < 32 && i < raw_message.size(); i++) {
                //     printf("%02X ", raw_message[i]);
                // }
                // std::cout << std::endl;
    
                
                {
                    std::lock_guard<std::mutex> lock(stats_mutex_);

                    size_t offset = 9;
                    
                    uint32_t received_high_net;
                    uint32_t received_low_net;

                    memcpy(&received_high_net, &raw_message[offset], 4);
                    stats_.received_lsn_high = ntohl(received_high_net);
                    offset += 4;
                    memcpy(&received_low_net, &raw_message[offset], 4);
                    stats_.received_lsn_low = ntohl(received_low_net);
                    offset += 4;

                    // std::stringstream ss;
                    // ss << "RECIEVED LSN [ " << std::hex << stats_.received_lsn_high << " / " << stats_.received_lsn_low << " ]" << std::dec << std::endl;
                    // std::cout << ss.str();
                }

                std::vector<uint8_t> clean_data(raw_message.begin() + 25, raw_message.end()); //need to parse lsn for wal msg end save to structure ? or skip this part ?
                // std::cout << "MSG TYPE HEAD < " << static_cast<char>(raw_message[25]) <<" > " << std::endl;
                auto message = parser_.parseMessage(clean_data);
                handleMessage(message);
                // std::cerr << "wal parse " << (char)clean_data[1] << std::endl;

                {
                    std::lock_guard<std::mutex> lock(stats_mutex_);
                    
                    stats_.applied_lsn_high = stats_.received_lsn_high;
                    stats_.applied_lsn_low = stats_.received_lsn_low;
                    
                    // std::stringstream ss2;
                    // ss2 << "APPLIED LSN [ " << std::hex << stats_.applied_lsn_high 
                    //     << " / " << stats_.applied_lsn_low << " ]" << std::dec << std::endl;
                    // std::cout << ss2.str();
                }
                
                sendStandbyStatusUpdate();
            }
            break;
        case 'k':
            {
                // std::cout << "MSG TYPE HEAD ================= < " << raw_message[0] << " > =========================================================== " << std::endl;
                sendStandbyStatusUpdate();
                break;
            }
        default:
            std::cerr << "DEFAULT SWITCH IN HEAD PARSE" << std::endl;
            break;
        }
    }
    catch(const std::exception& e)
    {
        //std::cerr << e.what() << '\n';
    }
}

void PostgresqlReplicator::sendStandbyStatusUpdate()
{
    // std::cerr << "sent answer" << std::endl;
    if(!repl_conn_ || PQstatus(repl_conn_) != CONNECTION_OK)
    {
        return;
    }
                                                        //    1            8               8               8               8            1 
    std::vector<char> reply(1 + 8 + 8 + 8 + 8 + 1, 0); // │  'r'    │  Flush LSN    │  Apply LSN    │  Flush LSN    │  Timestamp    │  Flag   │
    size_t pos = 0;
    reply[pos++] = 'r';

    {
        std::lock_guard<std::mutex> lock(stats_mutex_);

        stats_.restart_lsn_high = stats_.applied_lsn_high;
        stats_.restart_lsn_low = stats_.applied_lsn_low;
        // uint32_t lsn_high = 0;
        // uint32_t lsn_low = 0;
        uint32_t restart_high_net = htonl(stats_.restart_lsn_high);
        uint32_t restart_low_net = htonl(stats_.restart_lsn_low);
        uint32_t applied_high_net = htonl(stats_.applied_lsn_high);
        uint32_t applied_low_net = htonl(stats_.applied_lsn_low);

        memcpy(&reply[pos], &restart_high_net, 4); pos += 4;
        memcpy(&reply[pos], &restart_low_net, 4); pos += 4;
        // memcpy(&reply[pos], &stats_.lsn_end, 8);
        // pos += 8;

        memcpy(&reply[pos], &applied_high_net, 4); pos += 4;
        memcpy(&reply[pos], &applied_low_net, 4); pos += 4;
        // memcpy(&reply[pos], &stats_.lsn_end, 8);
        // pos += 8;
        
        memcpy(&reply[pos], &restart_high_net, 4); pos += 4;
        memcpy(&reply[pos], &restart_low_net, 4); pos += 4;
        // memcpy(&reply[pos], &stats_.lsn_end, 8);
        // pos += 8;
        
        // uint64_t timestamp = get_current_time_in_microseconds();
        // uint64_t net_timestamp = htobe64(timestamp);
        uint64_t timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
        uint64_t net_timestamp = htobe64(timestamp);
        memcpy(&reply[pos], &net_timestamp, 8); pos += 8;

        reply[pos++] = stats_.flag;
    }
    

    if (PQputCopyData(repl_conn_, reply.data(), reply.size()) != 1) {
        std::cerr << "Failed to send standby update" << std::endl;
    }
    
    PQflush(repl_conn_);
}


bool PostgresqlReplicator::initKafka(const std::string& brokers, const std::string& topic)
{
    kafka_producer_ = std::make_unique<LibrdKafkaProducer>();
    return kafka_producer_->init(brokers, topic);
}

uint64_t PostgresqlReplicator::currentTimestamp() const
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}



void PostgresqlReplicator::handleMessage(std::shared_ptr<pgoutput::ReplicationMessage> msg)
{
    switch (msg->getType())
    {
    case 'I':
        handleInsert(std::dynamic_pointer_cast<pgoutput::InsertMessage>(msg));
        break;
    case 'U':
        handleUpdate(std::dynamic_pointer_cast<pgoutput::UpdateMessage>(msg));
        break;
    case 'D':
        handleDelete(std::dynamic_pointer_cast<pgoutput::DeleteMessage>(msg));
        break;
    case 'T':
        handleTruncate(std::dynamic_pointer_cast<pgoutput::TruncateMessage>(msg));
        break;
    default:
        break;
    }
}

void PostgresqlReplicator::handleInsert(std::shared_ptr<pgoutput::InsertMessage> ins)
{
    std::cout << "HANDLE INSERT" << std::endl;

    auto relation = parser_.getCachedRelation(ins->getRelationId());

    if(!relation || !kafka_producer_)
    {
        return; // repeat send ???
    }

    Json::Value message;
    message["op"] = "INSERT";
    message["table"] = relation->getRelationName();
    message["schema"] = relation->getRelationNamespace();
    message["timestamp"] = static_cast<Json::Int64>(currentTimestamp());

    const auto &cols = relation->getColumns();
    Json::Value data;
    const auto &new_cols = ins->getNewTuple();

    for(size_t i = 0; i < new_cols.size(); ++i)
    {
        data[cols[i].name] = new_cols[i];
    }

    message["data"] = data;

    std::pair<std::string, std::string> primary_key = extractKey(data, cols);
    message["primary"] = primary_key.first;

    kafka_producer_->sendJson(message, primary_key.second);

    std::cout << "HANDLE INSERT DONE" << std::endl;
}

void PostgresqlReplicator::handleUpdate(std::shared_ptr<pgoutput::UpdateMessage> upd)
{
    std::cout << "HANDLE UPDATE" << std::endl;
    auto relation = parser_.getCachedRelation(upd->getRelationId());

    if(!relation || !kafka_producer_)
    {
        return; // repeat send ???
    }

    Json::Value message;
    message["op"] = "UPDATE";
    message["table"] = relation->getRelationName();
    message["schema"] = relation->getRelationNamespace();
    message["timestamp"] = static_cast<Json::Int64>(currentTimestamp());

    const auto &cols = relation->getColumns();
    Json::Value old_data;

    if(!upd->getOldTuples().empty())
    {
        Json::Value old_data;

        const auto &old_cols = upd->getOldTuples();
        for(size_t i = 0; i < old_cols.size(); ++i)
        {
            old_data[cols[i].name] = old_cols[i];
        }
        message["before"] = old_data;
    }

    const auto &new_cols = upd->getNewTuples();
    Json::Value new_data;

    for(size_t i = 0; i < new_cols.size(); ++i)
    {
        new_data[cols[i].name] = new_cols[i];
    }
    message["after"] = new_data;

    std::pair<std::string, std::string> primary_key = extractKey(new_data, cols);
    message["primary"] = primary_key.first;
    
    kafka_producer_->sendJson(message, primary_key.second);

    std::cout << "HANDLE UPDATE DONE" << std::endl;
}

void PostgresqlReplicator::handleTruncate(std::shared_ptr<pgoutput::TruncateMessage> trunc)
{
    std::cout << "HANDLE TRUNCATE" << std::endl;

    if(!kafka_producer_) return;

    Json::Value message;
    message["op"] = "TRUNCATE";
    message["timestamp"] = static_cast<Json::Int64>(currentTimestamp());

    Json::Value tables(Json::arrayValue);

    for(uint32_t oid : trunc->getRelationIds())
    {
        auto relation = parser_.getCachedRelation(oid);
        if(relation)
        {
            Json::Value table_info;
            
            table_info["schema"] = relation->getRelationNamespace();
            table_info["table"] = relation->getRelationName();
            tables.append(table_info);
        }
    }

    message["tables"] = tables;

    if(trunc->hasCascade() || trunc->hasRestartIdentity())
    {
        Json::Value options;
        if(trunc->hasCascade()) options["cascade"] = true;
        if(trunc->hasRestartIdentity()) options["restart_identity"] = true;
        message["options"] = options;
    }

    kafka_producer_->sendJson(message);

    std::cout << "HANDLE TRUNCATE DONE" << std::endl;
}

void PostgresqlReplicator::handleDelete(std::shared_ptr<pgoutput::DeleteMessage> del)
{
    std::cout << "HANDLE DELETE" << std::endl;

    auto relation = parser_.getCachedRelation(del->getRelationId());
    if(!relation || !kafka_producer_)
    {
        return; // repeat send ???
    }

    Json::Value message;
    message["op"] = "DELETE";
    message["table"] = relation->getRelationName();
    message["schema"] = relation->getRelationNamespace();
    message["timestamp"] = static_cast<Json::Int64>(currentTimestamp());

    const auto &cols = relation->getColumns();
    const auto &old_cols = del->getOldTuple();
    Json::Value old_data;

    for(size_t i = 0; i < old_cols.size(); ++i)
    {
        old_data[cols[i].name] = old_cols[i];
    }

    message["before"] = old_data;

    std::pair<std::string, std::string> primary_key = extractKey(old_data, cols);
    message["primary"] = primary_key.first;

    kafka_producer_->sendJson(message, primary_key.second);

    std::cout << "HANDLE DELETE DONE" << std::endl;
}





std::pair<std::string, std::string> PostgresqlReplicator::extractKey(const Json::Value& data, const std::vector<pgoutput::ReplicationMessage::Column>& cols) const
{
    for (size_t i = 0; i < cols.size(); ++i) {
        if (cols[i].flags & 1) {
            if (data.isMember(cols[i].name)) {
                Json::StreamWriterBuilder builder;
                builder["indentation"] = "";
                return {cols[i].name, Json::writeString(builder, data[cols[i].name])};
            }
        }
    }

    if (!cols.empty() && data.isMember(cols[0].name)) {
        Json::StreamWriterBuilder builder;
        builder["indentation"] = "";
        return {cols[0].name, Json::writeString(builder, data[cols[0].name])};
    }
    
    return {"", ""};
}

//---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
// bool PostgresqlReplicator::readReplicationMessage()
// {
//     if(!replicating_ || !repl_conn_)
//     {
//         std::cerr << "Failed to read replication message" << std::endl;
//         return false;
//     }
//     std::cerr << "смотрю данныe " << std::endl;

//     fd_set read_set;
//     FD_ZERO(&read_set);
//     FD_SET(PQsocket(repl_conn_), &read_set);

//     struct timeval timeout;
//     timeout.tv_sec = conninfo_.timeout_sec;
//     timeout.tv_usec = conninfo_.timeout_usec;

//     int ret = select(PQsocket(repl_conn_) + 1, &read_set, NULL, NULL, &timeout);
//     if(ret < 0)
//     {
//         perror("select() error");
//         std::cerr << "select() error" << std::endl;
//         return false;
//     }
//     if(ret == 0)
//     {
//         return true;
//     }
//     if(PQconsumeInput(repl_conn_) == 0)
//     {
//         std::cerr << "PQconsumeInput failed" << std::endl;
//         return false;
//     }

//     if(PQisBusy(repl_conn_))
//     {
//         PGresult *res = PQgetResult(repl_conn_);
//         if(!res)
//         {
//             std::cerr << "Break";
//             // break;
//         }

//         if(PQresultStatus(res) == PGRES_COPY_BOTH || PQresultStatus(res) == PGRES_COPY_OUT)
//         {
//             int rows = PQntuples(res);
//             for(int i = 0; i < rows; i++)
//             {
//                 char* json_data = PQgetvalue(res, i, 2);
//                 int data_len = PQgetlength(res, i, 2);
                
//                 if (json_data && data_len > 0) {
//                     std::string json_str(json_data, data_len);
//                     std::cerr << "Data check" << std::endl;
//                    // processWal2json(json_str);
//                 }
//             }
//         }
//         PQclear(res);
//     }
//     return true;
// }

// bool PostgresqlReplicator::processWal2json(const std::string &json_str)
// {
//     try
//     {   
//         Json::Value root; // object
//         Json::Reader reader; //из джейсона в object
//         Json::StreamWriterBuilder write_builder; //из object в джеймон
//         write_builder["indentation"] = "";

//         if(!reader.parse(json_str,root))
//         {
//             std::cerr << "JSON parsing error: {}" << reader.getFormattedErrorMessages();
//             return false;
//         }
//         if(root.isMember("change") && root["change"].isArray())
//         {
//             for(const auto &change : root["change"])
//             {
//                 processWal2jsonChange(change);
//             }
//         }
//         return true;
//     }
//     catch(const std::exception& e)
//     {
//         std::cerr << e.what() << '\n';
//         return false;
//     }
    
// }

// void PostgresqlReplicator::processWal2jsonChange(const Json::Value& change)
// {
//     std::string operation;
//     std::string table;
//     std::string schema;

//     if(change.isMember("kind"))
//     {
//         std::string kind = change["kind"].asString();

//         if(kind == "insert")
//         {
//             operation = "INSERT";
//             stats_.inserts++;
//         } else if(kind == "delete"){
//             operation = "DELETE";
//             stats_.deletes++;
//         } else if(kind == "update"){
//             stats_.updates++;
//         } else if(kind == "truncate"){
//             operation = "TRUNCATE";
//         } else if (kind == ""){

//         }
//     }

//     if(change.isMember("table"))
//     {
//         table = change["table"].asString();
//     }

//     if(change.isMember("schema"))
//     {
//         schema = change["schema"].asString();
//     }

//     Json::Value json_event;
//     json_event["table"] = table;
//     json_event["schema"] = schema;
//     json_event["operation"] = operation;
//     json_event["timestamp"] = static_cast<Json::Int64>(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());

//     if (change.isMember("columnnames") && change.isMember("columnvalues")) {
//         const Json::Value& names = change["columnnames"];
//         const Json::Value& values = change["columnvalues"];
        
//         Json::Value columns(Json::objectValue);
        
//         for(Json::ArrayIndex i = 0; i < names.size() && i < values.size(); ++i)
//         {
//             const std::string column_name = names[i].asString();

//             if(values[i].isString())
//             {
//                 columns[column_name] = values[i].asString();
//             } else if(values[i].isInt()){
//                 columns[column_name] = values[i].asInt();
//             } else if(values[i].isUInt()){
//                 columns[column_name] = values[i].asUInt();
//             } else if(values[i].isBool()){
//                 columns[column_name] = values[i].asBool();
//             } else if(values[i].isDouble()){
//                 columns[column_name] = values[i].asDouble();
//             } else if(values[i].isNull()){
//                 columns[column_name] = Json::nullValue;
//             } else{
//                 Json::StreamWriterBuilder writer;
//                 writer["indentation"] = "";
//                 columns[column_name] = Json::writeString(writer, values[i]);
//             }
//         }

//         if(operation == "INSERT" || operation == "UPDATE")
//         {
//             json_event["after"] = columns;
//         }

//         if(operation == "UPDATE" || operation == "DELETE" && change.isMember("oldkeys"))
//         {
//             const Json::Value &oldkeys = change["oldkeys"];
//             if(oldkeys.isMember("keynames") && oldkeys.isMember("keyvalues"))
//             {
//                 const Json::Value &old_names = oldkeys["keynames"];
//                 const Json::Value &old_values = oldkeys["keyvalues"];
//                 Json::Value old_columns(Json::objectValue);
//                 for(Json::ArrayIndex i = 0; i < old_names.size() && i < old_values.size(); ++i)
//                 {
//                     const std::string column_name = old_names[i].asString();

//                     if(old_values.isString())
//                     {
//                         old_columns[column_name] = old_values[i].asString();
//                     } else if(old_values.isInt()){
//                         old_columns[column_name] = old_values[i].asInt();
//                     } else if(old_values.isUInt()){
//                         old_columns[column_name] = old_values[i].asUInt();
//                     } else if(old_values.isBool()){
//                         old_columns[column_name] = old_values[i].asBool();
//                     } else if(old_values.isDouble()){
//                         old_columns[column_name] = old_values[i].asDouble();
//                     } else if(old_values.isNull()){
//                         old_columns[column_name] = Json::nullValue;
//                     } else {
//                         Json::StreamWriterBuilder writer;
//                         writer["indentation"] = "";
//                         old_columns[column_name] = Json::writeString(writer, old_values[i]);
//                     }
//                 }
//                 json_event["before"] = old_columns;
//             }
//         }
//     }
    
//     if(change.isMember("schema"))
//     {
//         json_event["schema"] = change["schema"].asString();
//     }

//     if(change.isMember("timestamp"))
//     {
//         json_event["postgres_timestamp"] = change["timestamp"].asString();
//     }

//     if(change.isMember("xid"))
//     {
//         json_event["transaction_id"] = change["xid"].asUInt();
//     }

//     {
//         std::lock_guard<std::mutex> lock(stats_mutex_);
//         stats_.total_changes++;
//     }

//     Json::StreamWriterBuilder writer;
//     writer["indentation"] = "";
//     std::string event_str = Json::writeString(writer, json_event);
// }


// bool PostgresqlReplicator::startReplication(const std::string &slot_name, const std::string &publication_name, std::vector<std::string> &tables)
// {
//     if(!conn_)
//     {
//         std::cerr << "Connetcting to database failed, check the connection" << std::endl;
//         return false;
//     }

//     if(!createReplicationSlot(slot_name))
//     {
//         return false;
//     }

//     if(!createPublication(publication_name, tables))
//     {
//         return false;
//     }

//     std::string conninfo = 
//         "host=" + std::string(PQhost(conn_)) + " " +
//         "port=" + std::string(PQport(conn_)) + " " +
//         "dbname=" + std::string(PQdb(conn_)) + " " +
//         "user=" + std::string(PQuser(conn_)) + " " +
//         "password=" + std::string(PQpass(conn_)) + " " +
//         "replication=" + conninfo_.database;

//     repl_conn_ = PQconnectdb(conninfo.c_str());
//     if(PQstatus(repl_conn_) != CONNECTION_OK)
//     {
//         std::cerr << "Replication connection failed: " << PQerrorMessage(repl_conn_) << std::endl;
//         return false;
//     }

//     std::string query = 
//     "START_REPLICATION SLOT " + slot_name + " LOGICAL 0/0";

//     PGresult *res = PQexec(repl_conn_, query.c_str());
//     if (PQresultStatus(res) != PGRES_COPY_BOTH) {
//     std::cerr << "START_REPLICATION failed: " << PQerrorMessage(repl_conn_) << std::endl;
//     PQclear(res);
//     return false;
//     }

//     PQclear(res);
//     replicating_ = true;
//     std::cout << "Replication started successfully" << std::endl;
    
//     return true;
// }

// bool PostgresqlReplicator::createPublication()
// {
//     std::string drop_query = "DROP PUBLICATION IF EXISTS " + conninfo_.publication_name;
//     PQexec(conn_, drop_query.c_str());

//     std::string tables_list = "";

//     for(const auto &table : conninfo_.tables)
//     {
//         if(!tables_list.empty())
//         {
//             tables_list += ", ";
//         }
//         tables_list += table;
//     }

//     std::string create_query = "CREATE PUBLICATION " + conninfo_.publication_name + " FOR TABLE " + tables_list;


//     PGresult *res = PQexec(conn_, create_query.c_str());
//     if(PQresultStatus(res) != PGRES_COMMAND_OK)
//     {
//         std::cerr << "Failed to create publication: " << PQerrorMessage(conn_) << std::endl;
//         PQclear(res);
//         return false;
//     }

//     PQclear(res);
//     std::cout << "Publication created: " << conninfo_.publication_name << std::endl;
//     return true;

// }