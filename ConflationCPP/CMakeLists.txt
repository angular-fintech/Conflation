cmake_minimum_required(VERSION 3.31)
project(ConflationCPP)

set(CMAKE_CXX_STANDARD 20)


find_package(rxcpp CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)


add_executable(ConflationCPP main.cpp
)

target_link_libraries(ConflationCPP PRIVATE rxcpp spdlog::spdlog)

