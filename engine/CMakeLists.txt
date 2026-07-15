cmake_minimum_required(VERSION 3.20)
project(hft_engine CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Default to Release if user didn't pick a config.
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
endif()

# ---- Global Compiler Settings ----
if(MSVC)
  # Static runtime: MT for Release, MTd for Debug. Avoids DLL hell.
  set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")

  # Base warnings + AVX2 optimizations for HFT code (_mm_pause, etc.)
  add_compile_options(/W4 /WX- /permissive- /arch:AVX2 /utf-8)
endif()

# ---- vcpkg packages ----
find_package(CURL REQUIRED)
find_package(hiredis CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(concurrentqueue CONFIG REQUIRED)
find_package(unofficial-sqlite3 CONFIG REQUIRED) 

# ---- Target Configuration ----
add_executable(engine main.cpp)

# Target-specific optimization flags (Semicolon-separated lists inside generator expressions)
target_compile_options(engine PRIVATE
    $<$<CONFIG:Release>:/O2;/Ob2;/Oi;/Ot;/DNDEBUG>
    $<$<CONFIG:RelWithDebInfo>:/O2;/Ob2;/Oi;/Ot;/DNDEBUG>
)

target_link_libraries(engine PRIVATE
    hiredis::hiredis
    CURL::libcurl
    nlohmann_json::nlohmann_json
    concurrentqueue::concurrentqueue
    unofficial::sqlite3::sqlite3    # Fixed typo here
    ws2_32                          # Windows sockets dependency for Redis/CURL
)

target_include_directories(engine PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})