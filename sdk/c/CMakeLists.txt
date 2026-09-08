cmake_minimum_required(VERSION 3.16)
project(matrix-component C CXX)

# Matrix C/C++ SDK (ML1, experimental 0.x). No third-party deps:
# C11 + POSIX (pthreads) for the C SDK; C++17 for the RAII header.
# Builds a static library (hermetic staging), the C/C++ test nodes,
# the doctor, and the self-tests.

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(MSVC)
  message(FATAL_ERROR "matrix-component targets POSIX (Linux); MSVC is not supported")
endif()

add_compile_options(-Wall -Wextra -Werror -pedantic)

add_library(matrix-component STATIC
  src/mx_json.c
  src/mx_component.c
  src/mx_op.c
)
target_include_directories(matrix-component PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)
target_link_libraries(matrix-component PUBLIC pthread)

add_executable(mx-node examples/node.c)
target_link_libraries(mx-node PRIVATE matrix-component)
target_include_directories(mx-node PRIVATE src)

add_executable(matrix-doctor examples/doctor.c)
target_link_libraries(matrix-doctor PRIVATE matrix-component)

enable_testing()
add_executable(test-component test/test_component.c)
target_link_libraries(test-component PRIVATE matrix-component)
target_include_directories(test-component PRIVATE src)
add_test(NAME component-loopback COMMAND test-component)

add_executable(test-operator test/test_operator.c)
target_link_libraries(test-operator PRIVATE matrix-component)
target_include_directories(test-operator PRIVATE src)
add_test(NAME operator-surface COMMAND test-operator)

if(MATRIX_ENABLE_CPP)
  add_executable(mx-node-cpp examples/node.cpp)
  target_link_libraries(mx-node-cpp PRIVATE matrix-component)
  target_include_directories(mx-node-cpp PRIVATE cpp include)
  add_executable(test-cpp test/test_cpp.cpp)
  target_link_libraries(test-cpp PRIVATE matrix-component)
  target_include_directories(test-cpp PRIVATE cpp include)
  add_test(NAME cpp-surface COMMAND test-cpp)
endif()

# Install layout (DESTDIR-friendly; used by package.sh staging too).
include(GNUInstallDirs)
install(TARGETS matrix-component mx-node matrix-doctor
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
install(FILES include/mx_component.h DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(FILES cpp/matrix.hpp DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/matrix-component.pc"
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig
)
configure_file(matrix-component.pc.in matrix-component.pc @ONLY)
