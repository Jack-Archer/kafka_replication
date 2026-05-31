#pragma once

#include <thread>

#include "database_writer.h"
#include "safe_queue.h"


#define QUEUE_SIZE 1000

template <typename T>
class ReplicationConsumer
{
    public:
        // ReplicationConsumer();
        ReplicationConsumer() : consumer_(nullptr), db_writer_(nullptr), all_started_(false), safe_queue_(QUEUE_SIZE)
        {
            db_writer_ = std::make_shared<DatabaseWriter>();
            consumer_ = std::make_shared<LibRdkafkaConsumer>();

            consumer_->setMessageCallback([this](const Json::Value& msg, const std::string& key) {
            std::cout << "  Callback: message with key=" << key << std::endl;
            std::cout << "  Operation: " << msg["op"].asString() << std::endl;
            std::cout << "  Table: " << msg["table"].asString() << std::endl;
            if (msg.isMember("data")) {
                std::cout << "  Data: " << msg["data"].toStyledString() << std::endl;
            }

            // std::cout << "  Primary  key is = " << msg[""] << std::endl;
            saveIntoQueue(msg); // из самого коллбэка забираем нужную нам информацию

            });

            consumer_->setErrorCallback([](const std::string& errstr){
                std::cerr << errstr << std::endl;
            });

            db_writer_thread_ = std::thread([this](){
                startDbWriter();
            });

            consumer_thread_ = std::thread([this](){
                startConsumer();
            });

            if(consumer_->status() && db_writer_->status())
            {
                all_started_ = true;
            } else
                {
                    if(!consumer_->status()) throw std::runtime_error("Consumer failed to start");
                    if(!db_writer_->status()) throw std::runtime_error("Database writer failed to start");
                }

            std::cout << "Consuming in progress ... .. ...  < status services  =  " << all_started_ << "   >   .. ... .. ..." << std::endl;
        }

        ~ReplicationConsumer() {
            all_started_ = false; 

            if (db_writer_thread_.joinable()) db_writer_thread_.join();
            if (consumer_thread_.joinable()) consumer_thread_.join();
        }

        //starting services consume <-> db writer

        void startConsumer()
        {
            consumer_->preStart();

            while(consumer_->status())
            {
                consumer_->startConsuming();
            }
        }




        void startDbWriter()
        {   
            while (db_writer_->status())
            {
                loadFromQueue();
            }
        }

        void saveIntoQueue(const Json::Value& msg)
        {
            ConsumerMessage consumer_msg;
            if(msg.isMember("primary"))
            {
               consumer_msg.primary_key = msg.get("primary", "").asString();
            }
            consumer_msg.primary_key = msg.get("primary", "").asString();
            consumer_msg.op = msg.get("op", "").asString();
            consumer_msg.table = msg.get("table", "").asString();

            std::cout << "  PRIMARY KEY < \"" <<  consumer_msg.primary_key << "\" >" << std::endl;

            if(consumer_msg.op == "INSERT")
            {
                consumer_msg.data = msg["data"];
            }else if(consumer_msg.op == "UPDATE")
                {
                    consumer_msg.before = msg["before"];
                    consumer_msg.after = msg["after"];
                } else if(consumer_msg.op == "DELETE")
                    {
                        consumer_msg.before = msg["before"];
                    }

            safe_queue_.push(consumer_msg);
            std::cout << "Add to queue : op = " << consumer_msg.op << "\n table = " << consumer_msg.table << std::endl;
            if (!consumer_msg.data.empty()) {
                std::cout << "  Data: " << msg["data"].toStyledString() << std::endl;
            }
        }

        void loadFromQueue()
        {
            ConsumerMessage data_to_save = safe_queue_.pop();
            
            db_writer_->handlerIncomingData(data_to_save);
            //начинаем сохранять изменения в головной базе куда реплицируем
            // if(msg_to_save.op == "INSERT")
            // {
            //     db_writer_->insertData(msg_to_save.get("data"));
            // }
            //..................................
        }

    private:
        std::shared_ptr<LibRdkafkaConsumer> consumer_;
        std::shared_ptr<DatabaseWriter> db_writer_;
        SafeQueue<T> safe_queue_;
        std::atomic<bool> all_started_;

        std::thread db_writer_thread_;
        std::thread consumer_thread_;
};
