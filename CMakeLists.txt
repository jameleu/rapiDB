cmake_minimum_required(VERSION 3.13)

project(redis-starter-cpp)

file(GLOB_RECURSE SOURCE_FILES src/*.cpp)

set(CMAKE_CXX_STANDARD 23)
set(THREADS_PREFER_PTHREAD_FLAG ON)

# Debug/Release settings
if (CMAKE_BUILD_TYPE MATCHES Debug)
    message(STATUS "Debug mode enabled")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic -g")
else()
    message(STATUS "Release mode enabled")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
endif()

find_package(Threads REQUIRED)
find_package(asio CONFIG REQUIRED)

add_executable(server ${SOURCE_FILES})

target_link_libraries(server PRIVATE asio asio::asio)
target_link_libraries(server PRIVATE Threads::Threads)

# Optional: Static linking
set(CMAKE_EXE_LINKER_FLAGS "-static")

# Optional: Install the executable
install(TARGETS server DESTINATION /usr/local/bin)
