#pragma once


#include <librdkafka/rdkafkacpp.h>
#include "../shared_structures.h"


#include <mutex>
#include <atomic>
#include <memory>
#include <functional>

class LibRdkafkaConsumer
{
    public:
        LibRdkafkaConsumer();
        ~LibRdkafkaConsumer();

        bool init(const std::string& brokers, const std::string& topic, const std::string &group_id = "replication_group");
        void preStart();
        void startConsuming();
        void stop();
        void setMessageCallback(std::function<void(const Json::Value&, const std::string&)> cb);
        void setErrorCallback(std::function<void(const std::string&)> cb);

        ConsumerStats getStats() const {return stats_;}

        const bool status() const;


    private:
        std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
        ConsumerStats stats_;
        ConnInfo conninfo_;
        mutable std::mutex stats_mutex_;
        std::atomic<bool> running_{false};
        std::string topic_name_;

        std::function<void(const Json::Value&, const std::string&)> message_callback_;
        std::function<void(const std::string&)> error_callback_;

};