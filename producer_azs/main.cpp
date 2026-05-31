#include "producer_librdkafka.h"
#include "postgresql_replicator.h"

#include <librdkafka/rdkafkacpp.h>
#include <iostream>
#include <csignal>
#include <atomic>
#include <chrono>
#include <thread>


int main()
{
    try
    {
        std::cout << "Compile successfuul" << std::endl;
    
        std::shared_ptr<PostgresqlReplicator> replicator{new PostgresqlReplicator};
        std::cerr << "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< Starting read messages >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" << std::endl;

        replicator->startReadReplicationMessages();
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    } catch (...){
        std::cerr << "THROW FROM INSIDE" << std::endl;
    }
    return 0;
}