#include <iostream>


#include "replication_consumer.h"


int main()
{
    try{
        std::shared_ptr<ReplicationConsumer<ConsumerMessage>> consumer_full = std::make_shared<ReplicationConsumer<ConsumerMessage>>();

    } catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    } catch (...)
    {
        std::cerr << "THROW FROM INSIDE" << std::endl;
    }
    
    return 0;
}