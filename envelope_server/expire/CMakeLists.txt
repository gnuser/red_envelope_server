cmake_minimum_required(VERSION 2.8)
project(envelope_expired)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set(CMAKE_BUILD_TYPE "Debug")
include_directories(./)

set(main_src ./main.cpp)

add_executable(envelope_expired ${main_src})
target_link_libraries(envelope_expired curl ev)

