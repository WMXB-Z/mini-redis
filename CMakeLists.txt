cmake_minimum_required(VERSION 3.15)

project(mini_redis LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(MINI_REDIS_ENABLE_WARNINGS "Enable compiler warnings" ON)
find_package(Threads REQUIRED)

if(MINI_REDIS_ENABLE_WARNINGS)
  if (CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    add_compile_options(-Wall -Wextra -Wpedantic -Wconversion)
  elseif (CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    add_compile_options(/W4)
  endif()
endif()

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Release")
  add_compile_options(-O3 -DNDEBUG)
endif()

add_executable(mini_redis
  src/main.cpp
  src/server.cpp
  src/resp.cpp
  src/kv.cpp
  src/config_loader.cpp
  src/aof.cpp
  src/rdb.cpp
  src/replica_client.cpp
)

target_compile_definitions(mini_redis PRIVATE $<$<CONFIG:Debug>:MINI_REDIS_DEBUG=1>)

target_include_directories(mini_redis PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

install(TARGETS mini_redis RUNTIME DESTINATION bin)

target_link_libraries(mini_redis PRIVATE Threads::Threads)


