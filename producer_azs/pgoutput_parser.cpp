#include <iostream>

#include "pgoutput_parser.h"

#include <arpa/inet.h>

#include <string.h>

uint8_t pgoutput::PgOutputParser::read_uint8(const std::vector<uint8_t>& data, size_t& offset)
{
    if (offset >= data.size()) {
        throw std::runtime_error("Buffer underflow in read_uint8");
    }
    return data[offset++];
}

uint16_t pgoutput::PgOutputParser::read_uint16(const std::vector<uint8_t>& data, size_t& offset)
{
    if (offset + 2 > data.size()) {
        throw std::runtime_error("Buffer underflow in read_uint8");
    }
    
    uint16_t bytes_read;
    memcpy(&bytes_read, &data[offset], 2);
    offset += 2;
    return ntohs(bytes_read);
}

uint32_t pgoutput::PgOutputParser::read_uint32(const std::vector<uint8_t>& data, size_t& offset)
{
    if (offset + 4 > data.size()) {
        throw std::runtime_error("Buffer underflow in read_uint8");
    }

    uint32_t bytes_read;
    memcpy(&bytes_read, &data[offset], 4);
    offset += 4;
    return ntohl(bytes_read);
}

uint64_t pgoutput::PgOutputParser::read_uint64(const std::vector<uint8_t>& data, size_t& offset)
{
    if (offset + 8 > data.size()) {
        throw std::runtime_error("Buffer underflow in read_uint8");
    }

    uint64_t lsn_full = 0;

    uint32_t lsn_high = read_uint32(data, offset);
    uint32_t lsn_low = read_uint32(data, offset);

    lsn_full = (static_cast<uint64_t>(lsn_high) << 32) | lsn_low;

    return lsn_full;
}

std::string pgoutput::PgOutputParser::read_string(const std::vector<uint8_t>& data, size_t& offset)
{
    if (offset >= data.size()) {
        throw std::runtime_error("Buffer underflow in read_string start");
    }

    std::string str;

    while(offset < data.size() && data[offset] != '\0')
    {
        str.push_back(data[offset++]);
    }

    if(offset >= data.size()) {
        throw std::runtime_error("String not null-terminated");
    }
    
    offset++;

    return str;
}

int64_t pgoutput::PgOutputParser::read_int64(const std::vector<uint8_t>& data, size_t& offset)
{
    if (offset + 8 > data.size()) {
        throw std::runtime_error("Buffer underflow in read_uint8");
    }

    uint64_t bytes_read;
    memcpy(&bytes_read, &data[offset], 8);
    offset += 8;

    return be64toh(bytes_read);
}


std::shared_ptr<pgoutput::ReplicationMessage> pgoutput::PgOutputParser::parseMessage(const std::vector<uint8_t> raw_message)
{
    char msg_type = static_cast<char>(raw_message[0]);
    // std::cout << "MSG TYPE = " << msg_type << std::endl;
    std::vector<uint8_t> clean_data(raw_message.begin() + 1, raw_message.end());

    std::shared_ptr<ReplicationMessage> message;

    try
    {
        switch (msg_type)
        {
        case 'B':
            {
                // std::cerr << std::endl << ">>>>>>>>>>>>>>>>>>>>>>>>>>> PARSE BEGIN <<<<<<<<<<<<<<<<<<<<<<<<<" << std::endl;
                auto beg_msg = std::make_shared<BeginMessage>();
                beg_msg->parse(clean_data, this);
                message = beg_msg;
            }
            break;
        case 'C':
            {
                // std::cerr << std::endl << ">>>>>>>>>>>>>>>>>>>>>>>>>>> PARSE COMMIT <<<<<<<<<<<<<<<<<<<<<<<<<" << std::endl;
                auto com_msg = std::make_shared<CommitMessage>();
                com_msg->parse(clean_data, this);
                message = com_msg;
            }
            break;
        case 'R':
            {
                // std::cerr << std::endl << ">>>>>>>>>>>>>>>>>>>>>>>>>>> PARSE RELATION <<<<<<<<<<<<<<<<<<<<<<<<<" << std::endl;
                auto rel_msg = std::make_shared<RelationMessage>();
                rel_msg->parse(clean_data, this);
                cacheRelation(rel_msg);
                // getRelationInfo();
                message = rel_msg;   
            }
            break;
        case 'I':
            {
                // std::cerr << std::endl << ">>>>>>>>>>>>>>>>>>>>>>>>>>> PARSE INSERT <<<<<<<<<<<<<<<<<<<<<<<<<" << std::endl;
                auto ins_msg = std::make_shared<InsertMessage>();
                ins_msg->parse(clean_data, this);
                message = ins_msg;
            }
            break;
        case 'U':
            {
                // std::cerr << std::endl << ">>>>>>>>>>>>>>>>>>>>>>>>>>> PARSE UPDATE <<<<<<<<<<<<<<<<<<<<<<<<<" << std::endl;
                auto upd_msg = std::make_shared<UpdateMessage>();
                upd_msg->parse(clean_data, this);
                message = upd_msg; 
            }
            break;
        case 'D':
            {
                // std::cerr << std::endl << ">>>>>>>>>>>>>>>>>>>>>>>>>>> PARSE DELETE <<<<<<<<<<<<<<<<<<<<<<<<<" << std::endl;
                auto del_msg = std::make_shared<DeleteMessage>();
                del_msg->parse(clean_data, this);
                message = del_msg;
            }
            break;
        case 'T':
            {
                // std::cerr << std::endl << ">>>>>>>>>>>>>>>>>>>>>>>>>>> PARSE TRUNCATE <<<<<<<<<<<<<<<<<<<<<<<<<" << std::endl;
                auto tr_msg = std::make_shared<TruncateMessage>();
                tr_msg->parse(clean_data, this);
                message = tr_msg;
            }
            break;
        default:
            std::cerr << "DEFAULT SWITCH" << std::endl;
            break;
        }
    }
    catch(const std::exception& e)
    {
        //std::cerr << e.what() << '\n';
    }
    return message;
}

void pgoutput::PgOutputParser::cacheRelation(const std::shared_ptr<RelationMessage>& relation)
{
    relation_cache_[relation->getRelationId()] = relation;
}
// size_t pgoutput::PgOutputParser::getRelationSize(const std::vector<uint8_t>& msg)
// {
//     if(msg.size() < 5) // Минимум: тип + OID
//     {
//         return 0;
//     }

//     size_t offset = 1;
//     offset += 4;

//     while(offset < msg.size() && msg[offset != 0])
//     {
//         offset++; 
//     }

//     if (offset >= msg.size()) return 0;

//     offset++;

//     while(offset < msg.size() && offset != 0)
//     {
//         offset++;
//     }

//     if (offset >= msg.size()) return 0;
//     offset++;

//     if (offset >= msg.size()) return 0;
//     offset++;
    
//     if (offset + 2 >= msg.size()) return 0;
//     offset += 2;

//     return offset;
// }

std::vector<std::string> pgoutput::PgOutputParser::parseTuple(const std::vector<uint8_t>& data, size_t& offset)
{
    uint16_t num_columns = read_uint16(data, offset);
    std::vector<std::string> tuple;

    for(uint16_t i = 0; i < num_columns; ++i)
    {
        uint8_t column_type = read_uint8(data, offset);

        if(column_type == 'n')
        {
            tuple.push_back("NULL");
        } else if(column_type == 'u')
            {
                tuple.push_back("TOAST");
            } else if(column_type == 't')
                {
                    uint32_t length = read_uint32(data, offset);
                    std::string value;
                    if(length > 0)
                    {
                        value.assign(reinterpret_cast<const char *>(&data[offset]), length);
                        offset += length;
                    }
                    tuple.push_back(value);
                    // иногда есть бинарные данные 'b' пока не парсил написано что это очень редкий формат который нужно запрашивать отдельно при запуске репликации
                }
    }

    return tuple;
}

std::vector<std::string> pgoutput::PgOutputParser::parseTuple(const std::vector<uint8_t>& data, size_t& offset, const std::vector<pgoutput::ReplicationMessage::Column>& columns)
{
    uint16_t num_values = read_uint16(data, offset);
    std::vector<std::string> tuple;
    tuple.reserve(num_values);

    if (num_values != columns.size()) {
        std::cout << "Partial tuple: got " << num_values 
                  << " values, relation has " << columns.size() << std::endl;
    }

    for(uint16_t i = 0; i < num_values; ++i)
    {
        uint8_t column_type = read_uint8(data, offset);
        std::string value;

        if(column_type == 'n')
        {
            value = "NULL";
        } 
        else if(column_type == 'u')
        {
            value = "TOAST";
        } 
        else if(column_type == 't')
        {
            uint32_t length = read_uint32(data, offset);
            if(length > 0)
            {
                value.assign(reinterpret_cast<const char*>(&data[offset]), length);
                offset += length;
            }
        }
        
        tuple.push_back(value);
        
        // if (i < columns.size()) {
        //     validateValueByType(value, columns[i]);
        // }
    }

    return tuple;
}




    void pgoutput::PgOutputParser::getRelationInfo()
    {
        std::stringstream ss;
        for(const auto &rc : relation_cache_)
        {
            ss << "Relation cache [ " << rc.first;
            ss << ", " << rc.second->getType() << " ]" << std::endl; 
        }
        std::cout << ss.str();
    }


    std::shared_ptr<pgoutput::RelationMessage> pgoutput::PgOutputParser::getCachedRelation(uint32_t relation_id)
    {
        auto it = relation_cache_.find(relation_id);

        if(it != relation_cache_.end())
        {
            return it->second;
        }
        return nullptr;
    }