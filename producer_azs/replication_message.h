#pragma once



#include <vector>
#include <cstdint>
#include <string>

#include "../shared_structures.h"

namespace pgoutput {

    class PgOutputParser;

//##################################################### INTERFACE MSG ########################################################
    class ReplicationMessage
    {
        public:
            virtual ~ReplicationMessage() = default;
            virtual void parse(const std::vector<uint8_t> &data,  PgOutputParser *parser) = 0;
            virtual std::string toString() const = 0;
            virtual char getType() const = 0;

        struct Column {

            uint8_t flags;
            std::string name;
            uint32_t type_id;
            int32_t type_modifier;
            
            std::string toString() const {
                return name + " (type: " + std::to_string(type_id) + 
                    ", modifier: " + std::to_string(type_modifier) + ")";
            }
        };
    };

//##################################################### BEGIN MSG ########################################################
    class BeginMessage : public ReplicationMessage
    {
        public:
            void parse(const std::vector<uint8_t> &data,  PgOutputParser *parser) override;
            std::string toString() const override;
            char getType() const override {
                return 'B';
            }

            uint64_t get_lsn() const
            {
                return lsn_;
            }
            
            uint64_t get_timestamp() const
            {
                return commit_timestamp_;
            }

        private:
            uint64_t lsn_;
            uint64_t commit_timestamp_;
            uint32_t transaction_id_;
    };

//##################################################### RELATION MSG ########################################################
    class RelationMessage : public ReplicationMessage
    {
        public:
            void parse(const std::vector<uint8_t> &data,  PgOutputParser *parser) override;
            std::string toString() const override;
            char getType() const override {
                return 'R';
            }
            uint32_t getRelationId() const
            {
                return relation_id_;
            }

            std::string getRelationName() const
            {
                return relation_name_;
            }

            std::vector<Column> getColumns() const
            {
                return columns_;
            }

            std::string getRelationNamespace() const
            {
                return relation_name_;
            }

        private:
            uint32_t relation_id_;
            std::string namespace_;
            std::string relation_name_;
            char replica_identity_;
            std::vector<Column> columns_;
    };

//##################################################### COMMIT MSG ########################################################
    class CommitMessage : public ReplicationMessage {
        private:
            uint8_t flags_;
            uint64_t lsn_;
            uint64_t end_lsn_;
            uint64_t commit_timestamp_;
            
        public:
            void parse(const std::vector<uint8_t>& data,  PgOutputParser *parser) override;
            std::string toString() const override;
            char getType() const override { return 'C'; }
        };

//##################################################### INSERT MSG ########################################################
    class InsertMessage : public ReplicationMessage
    {
        public:
            void parse(const std::vector<uint8_t>& data, PgOutputParser *parser) override;
            std::string toString() const override;
            char getType() const override { return 'I'; }
        
            uint32_t getRelationId() const { return relation_id_; }
            const std::vector<std::string>& getNewTuple() const { return new_tuple_; }

        private:
            uint32_t relation_id_;
            char tuple_type_;
            std::vector<std::string> new_tuple_;
    };

//##################################################### DELETE MSG ########################################################
    class DeleteMessage : public ReplicationMessage
    {
        public:
            void parse(const std::vector<uint8_t>& data, PgOutputParser *parser) override;
            std::string toString() const override;
            char getType() const override { return 'D'; }
            uint32_t getRelationId() const {return relation_id_;}
            const std::vector<std::string>& getOldTuple() const { return old_tuple_; }

        private:
            uint32_t relation_id_;
            std::vector<std::string> old_tuple_;
    };

//##################################################### UPDATE MSG ########################################################
    class UpdateMessage : public ReplicationMessage
    {
        public:
            void parse(const std::vector<uint8_t>& data, PgOutputParser *parser) override;
            std::string toString() const override;
            char getType() const override { return 'U'; }
            uint32_t getRelationId() const {return relation_id_;}
            std::vector<std::string> getOldTuples() const {return old_tuple_;}
            std::vector<std::string> getNewTuples() const {return new_tuple_;}
        private:
            uint32_t relation_id_;
            std::vector<std::string> old_tuple_;
            std::vector<std::string> new_tuple_;
    };

//##################################################### TRUNCATE MSG ########################################################
    class TruncateMessage : public ReplicationMessage {
        public:
            void parse(const std::vector<uint8_t>& data, PgOutputParser *parser) override;
            std::string toString() const override;
            char getType() const override { return 'T'; }
            std::vector<uint32_t> getRelationIds() const {return relation_ids_;}
            bool hasCascade() const {return (options_ & 0x01) != 0;}
            bool hasRestartIdentity() const {return (options_ & 0x02) != 0;}
            
        private:
            uint32_t transaction_id_;
            uint32_t number_of_relations_;
            uint8_t options_;
            std::vector<uint32_t> relation_ids_;
    };


};