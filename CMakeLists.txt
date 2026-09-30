cmake_minimum_required(VERSION 3.22)
project(consensus-protocol-lab VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

add_library(consensus_lab_core src/version.cpp)
target_include_directories(consensus_lab_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_compile_features(consensus_lab_core PUBLIC cxx_std_20)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  target_compile_options(consensus_lab_core PRIVATE -Wall -Wextra -Wpedantic)
endif()

add_executable(consensus-protocol-lab src/main.cpp)
target_link_libraries(consensus-protocol-lab PRIVATE consensus_lab_core)

include(CTest)
if(BUILD_TESTING)
  add_subdirectory(tests)
endif()
