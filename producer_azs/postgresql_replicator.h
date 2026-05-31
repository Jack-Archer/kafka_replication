#pragma once

#include <functional>
#include <string>
#include <libpq-fe.h>
#include <atomic>
#include <chrono>
#include <mutex>
#include <json/json.h>
#include <memory>

#include "pgoutput_parser.h"
#include "producer_librdkafka.h"
#include "config_manager.h"

#define PATH_TO_CONFIG "../replicator.conf"

class PostgresqlReplicator
{
    public:
        // using CallBack = std::function<void(const std::string &table, std::string &operation, std::string &data_json)>;

        PostgresqlReplicator();
        ~PostgresqlReplicator();

        bool connect();
        // std::string getEnvLocal(const char *env) const;

        // bool startReplication(const std::string &slot_name, const std::string &publication_name, std::vector<std::string> &tables);
        bool startReplicationAll();
        void stopReplication();
        void startReadReplicationMessages();

        //producer
        bool initKafka(const std::string& brokers, const std::string& topic);

        void handleMessage(std::shared_ptr<pgoutput::ReplicationMessage> msg);
        void handleInsert(std::shared_ptr<pgoutput::InsertMessage> ins);
        void handleUpdate(std::shared_ptr<pgoutput::UpdateMessage> upd);
        void handleTruncate(std::shared_ptr<pgoutput::TruncateMessage> trunc);
        void handleDelete(std::shared_ptr<pgoutput::DeleteMessage> del);
       
        // bool readReplicationMessage();
        // void setCallback(CallBack cb){callback_ = cb;}

        // bool isConecting() const
        // {
        //     return conn_ != nullptr;
        // }

        // bool isReplicating() const
        // {
        //     return replicating_;
        // }
        uint64_t currentTimestamp() const;
        std::pair<std::string, std::string>  extractKey(const Json::Value& data, const std::vector<pgoutput::ReplicationMessage::Column>& cols) const;

        private:
            // CallBack callback_;
            std::atomic<bool> replicating_; //replication status
            std::unique_ptr<LibrdKafkaProducer> kafka_producer_;
            ConfigManager config_manager_;
            PGconn *conn_; // local DB on azs machine
            PGconn *repl_conn_; //replication pointer
            ConnInfo conninfo_; //parameters of BD on azs machine
            pgoutput::PgOutputParser parser_; //main parser
            Stats stats_; // заполнить действуещей точкой старта и точкой с которой начинаем
            std::mutex stats_mutex_;

            bool createReplicationSlot();
            // bool createPublication();
            bool createPublicationAllTables();
            void processReplicationMessage(const char *buff, int read_bytes);
            void parsePacket(const std::vector<uint8_t> raw_message);
            void sendStandbyStatusUpdate();
            // bool processWal2json(const std::string &json_str);
            // void processWal2jsonChange(const Json::Value& change);
            // bool processWal2JsonMessage(const char* buffer, size_t length);
};