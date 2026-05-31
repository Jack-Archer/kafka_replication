#include <exception>
#include <iostream>
#include <fstream>




#include "config_manager.h"


ConfigManager::ConfigManager(const std::string &filename)
{
    if(!filename.empty())
    {
        filename_ = filename;
        std::cout << "FILENAME = " << filename_ << std::endl;
    } else {
        throw std::runtime_error("Path to config file don't set");
    }
    load();
}

ConfigManager::~ConfigManager()
{

}

void ConfigManager::load()
{
    std::ifstream file(filename_);
    if(!file.is_open())
    {
        std::cerr << "Config file not found from path "<< filename_ << " , using defaults" << std::endl;
        return;
    }
    std::string line;
    while(std::getline(file, line))
    {
        if(line.empty() || line[0] == '#') continue;

        std::string value = "";
        std::string key = "";
        size_t eq_pos = line.find('=');
        if(eq_pos == std::string::npos) continue;
        key = line.substr(0, eq_pos);
        if(line.size() >= eq_pos +1) value = line.substr(eq_pos + 1);

        size_t key_start = key.find_first_not_of(" \t\n\r");
        size_t key_end = key.find_last_not_of(" \t\n\r");
        if(key_start == std::string::npos) continue;
        key = key.substr(key_start, key_end - key_start + 1);

        size_t value_start = value.find_first_not_of(" \t\n\r");
        size_t value_end = value.find_last_not_of(" \t\n\r");
        if(value_start == std::string::npos)
        {
            value = "";
        } else{
            value = value.substr(value_start, value_end - value_start + 1);
        }
        // std::cout << "KEY = " << key << "\n VALUE = " << value << std::endl;
        values_[key] = value;
    }

    file.close();
}

std::string ConfigManager::getValueByKeyFromConfig(const std::string &key) const
{
    auto it = values_.find(key);
    if(it == values_.end())
    {
        std::cout << "The config file not contains this field, used default value" << std::endl;
        return "";
    }
    
    return it->second;
}


void ConfigManager::parseLSN(const std::string& lsn_str, uint32_t& high, uint32_t& low)
{
    size_t slash = lsn_str.find('/');
    if(slash == std::string::npos)
    {
        high = 0;
        low = 0;
        return;
    }

    high = std::stoul(lsn_str.substr(0, slash));
    low = std::stoul(lsn_str.substr(slash + 1));
}


void ConfigManager::save(const std::string &key, const std::string &value)
{

}
