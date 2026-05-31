cmake_minimum_required(VERSION 3.1)
project(rdkafka_consumer)

find_package(PostgreSQL REQUIRED)
find_package(jsoncpp REQUIRED)
find_package(CURL REQUIRED)

find_library(RDKAFKACPP_LIB rdkafka++ REQUIRED)
find_library(RDKAFKA_LIB rdkafka REQUIRED)
find_path(RDKAFKA_INCLUDE_DIR librdkafka/rdkafkacpp.h)


set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(rdkafka_consumer
    consumer_librdkafka.cpp
    database_writer.cpp
    main.cpp
)

target_include_directories(rdkafka_consumer PRIVATE
    ${PostgreSQL_INCLUDE_DIRS}
    ${JSONCPP_INCLUDE_DIRS}
    ${CURL_INCLUDE_DIRS}
    ${RDKAFKA_INCLUDE_DIR}
)


target_link_libraries(rdkafka_consumer PRIVATE
    PostgreSQL::PostgreSQL
    JsonCpp::JsonCpp
    CURL::libcurl
    ${RDKAFKA_LIB}
    ${RDKAFKACPP_LIB}
)