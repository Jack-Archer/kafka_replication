#pragma once


// #define HOST "localhost"
// #define PORT 5432
// #define DBNAME "test_db"
// #define USER "mcst"
// #define PASSWORD "station"
// #define SLOT_NAME "kafka_replication_slot"
// #define PUBLICATION_NAME "kafka_publication"
// #define OUTPUT_PLUGIN "pgoutput"
// #define PROTO_VERSION "4"
// #define BROKER "localhost:9092"
// #define TOPIC "postgres_changes"

#include <arpa/inet.h>
#include <string>
#include <vector>
#include <iostream>
#include <json/json.h>


struct ConsumerMessage
{
    std::string primary_key;
    std::string op;
    std::string table;
    Json::Value data;
    Json::Value before;
    Json::Value after;
};


// enum MessageType { // ?????
//     BEGIN = 'B',
//     COMMIT = 'C',
//     INSERT = 'I',
//     UPDATE = 'U',
//     DELETE = 'D',
//     RELATION = 'R',
//     TYPE = 'Y',
//     ORIGIN = 'O',
//     TRUNCATE = 'T'
// };

//нужен ли каррет ЛСН который подтвержденно отправился в апачи
struct Stats {
    uint32_t restart_lsn_high{0};
    uint32_t restart_lsn_low{0};
    
    uint32_t applied_lsn_high{0};    // что уже применили к данным
    uint32_t applied_lsn_low{0};
    
    uint32_t received_lsn_high{0};   // что получили от сервера
    uint32_t received_lsn_low{0};
    
    uint64_t last_heartbeat{0};      // время последнего подтверждения
    // uint32_t flush_lsn_high{0};
    // uint32_t flush_lsn_low{0};
    // uint32_t apply_lsn_high{0};
    // uint32_t apply_lsn_low{0};
    // uint32_t recieved_lsn_high{0};
    // uint32_t recieved_lsn_low{0};
    uint64_t timestamp{0}; //poka prosto zapisal chtobi ne zabyt'
    uint8_t flag{0}; //poka prosto zapisal chtobi ne zabyt'

    Stats() = default;
    // uint64_t loadOnRestart()
    // {
    //     return 0;
    //     // restart_lsn_high // get from disk
    //     // restart_lsn_low //get from disk
    //     //and return like a start lsn to start replicaton
    // }
    // Stats(uint32_t fhigh, uint32_t flow, uint32_t ahigh, uint32_t alow, uint64_t timestamp_, uint8_t flag_) 
    // {
    //     flush_lsn_high = fhigh;
    //     flush_lsn_low = flow;
    //     apply_lsn_high = ahigh;
    //     apply_lsn_low = alow;
    //     timestamp = timestamp_;
    //     flag = flag_;
    // }
};

struct ProducerStats
{
    uint64_t messages_sent{0};
    uint64_t bytes_sent{0};
    uint64_t errors{0};

    ProducerStats() = default;
};


struct ConsumerStats{
    uint64_t message_received{0};
    uint64_t bytes_received{0};
    uint64_t errors{0};
};


struct ConnInfo
{
        std::string host;
        int port;
        std::string db_name;
        std::string db_user;
        std::string db_password;
        std::string database{}; // default what
        __time_t timeout_sec = 0; // deafault what
        __suseconds_t timeout_usec = 100000; //default what
        
        ~ConnInfo(){};
        ConnInfo()
        {
            host =  getEnvLocal("HOSTNAME");
            port = std::stoi(getEnvLocal("MCC_DB_PORT"));
            db_name = getEnvLocal("DBNAME_REPLICA");
            db_user = getEnvLocal("MCC_DB_USER");
            db_password = getEnvLocal("MCC_DB_PASSWORD");
            slot_name = getEnvLocal("SLOT_NAME");
            publication_name = getEnvLocal("PUBLICATION_NAME");
            output_plugin = getEnvLocal("OUTPUT_PLUGIN");
            proto_version = getEnvLocal("PROTO_VERSION");
            brokers_names = getEnvLocal("BROKERS");
            topic = getEnvLocal("TOPIC");
            group_id = getEnvLocal("GROUP_ID_KAFKA");
        }

        std::string getEnvLocal(const char *env) const
        {   
            const char *env_get = std::getenv(env);
            std::string env_get_str(env_get);
            if(env_get == nullptr)
            {
                std::cerr << "One of env is empty :: " << env_get_str << std::endl;
                throw  std::runtime_error("Failed to read some envs");
            } else
                {
                    std::cout << "Env set :: " << env_get_str << std::endl;
                    return env_get_str;
                }
        }

        std::string concateToString() const
        {
            std::string conninfo = "host=" + this->host + " " +
            "port=" + std::to_string(this->port) + " " +
            "dbname=" + this->db_name + " " +
            "user=" + this->db_user + " " +
            "password=" + this->db_password;
            return conninfo;
        }

        // Настройки репликации
        std::string brokers_names;
        std::string topic;
        std::string slot_name;
        std::string output_plugin; //"wal2json";
        std::string publication_name;
        std::string proto_version;
        std::string group_id;
        
        std::vector<std::string> tables;
        
        // Настройки WAL
        bool include_transaction = true;
        bool include_timestamp = true;
        bool include_schema = false;
        bool include_pk = true;
        
        // Настройки производительности
        int wal_sender_timeout = 60;  // секунды
        int max_wal_senders = 10;
        size_t batch_size = 1000;     // сообщений
};