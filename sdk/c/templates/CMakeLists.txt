cmake_minimum_required(VERSION 3.16)
# Scaffolded Matrix node project (ML1): vendored SDK + node binary.
# Offline build, no packages: `cmake -B build && cmake --build build`.
project(node C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
add_compile_options(-Wall -Wextra -Werror -pedantic)

add_library(matrix-component STATIC
  src/mx_json.c
  src/mx_component.c
  src/mx_op.c
)
target_include_directories(matrix-component PUBLIC include)
target_include_directories(matrix-component PRIVATE src)
target_link_libraries(matrix-component PUBLIC pthread)

add_executable(mx-node node.c)
target_link_libraries(mx-node PRIVATE matrix-component)
target_include_directories(mx-node PRIVATE src)

if(MATRIX_ENABLE_CPP)
  add_executable(mx-node-cpp node.cpp)
  target_link_libraries(mx-node-cpp PRIVATE matrix-component)
  target_include_directories(mx-node-cpp PRIVATE cpp include)
endif()
