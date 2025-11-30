cmake_minimum_required(VERSION 3.14)
project(joy VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Compiler warnings
if(MSVC)
    add_compile_options(/W4)
else()
    add_compile_options(-Wall -Wextra -pedantic)
endif()

include_directories(${PROJECT_SOURCE_DIR}/include)

# Library containing all core components
add_library(joylib
    src/lexer.cpp
    src/parser.cpp
    src/compiler.cpp
    src/vm.cpp
    src/table.cpp
)

# Main executable
add_executable(joy src/main.cpp)
target_link_libraries(joy joylib)

# Test executables (optional for MVP)
add_executable(test_lexer tests/test_lexer.cpp)
target_link_libraries(test_lexer joylib)

add_executable(test_parser tests/test_parser.cpp)
target_link_libraries(test_parser joylib)

add_executable(test_table tests/test_table.cpp)
target_link_libraries(test_table joylib)

add_executable(test_compiler tests/test_compiler.cpp)
target_link_libraries(test_compiler joylib)

add_executable(test_vm tests/test_vm.cpp)
target_link_libraries(test_vm joylib)
