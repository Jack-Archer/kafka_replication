cmake_minimum_required(VERSION 3.1)

project(rdkafka_producer)

find_package(PostgreSQL REQUIRED)
find_package(jsoncpp REQUIRED)
find_package(CURL REQUIRED)

find_library(RDKAFKACPP_LIB rdkafka++ REQUIRED)
find_library(RDKAFKA_LIB rdkafka REQUIRED)
find_path(RDKAFKA_INCLUDE_DIR librdkafka/rdkafkacpp.h)


set (CMAKE_CXX_STANDARD 17)
set (CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(rdkafka_producer
    postgresql_replicator.cpp
    producer_librdkafka.cpp
    pgoutput_parser.cpp
    replication_message.cpp
    config_manager.cpp
    main.cpp
)


target_include_directories(rdkafka_producer PRIVATE
    ${CPPKAFKA_INCLUDE_DIRS}
    ${JSONCPP_INCLUDE_DIRS}
    ${CURL_INCLUDE_DIRS}
    ${RDKAFKA_INCLUDE_DIR}
)
target_link_libraries(rdkafka_producer
    PostgreSQL::PostgreSQL
    JsonCpp::JsonCpp
    CURL::libcurl
    ${RDKAFKA_LIBRARIES}
    ${CPPKAFKA_LIBRARIES}
    ${RDKAFKACPP_LIB}
    ${RDKAFKA_LIB}
    jsoncpp
)
