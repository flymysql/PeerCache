cmake_minimum_required(VERSION 3.18)
project(peercache LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

option(PEERCACHE_NO_RDMA "Build without libibverbs/librdmacm (stub transport)" OFF)

find_package(pybind11 CONFIG REQUIRED)

# Try to locate the RDMA core libraries unless explicitly disabled.
if(NOT PEERCACHE_NO_RDMA)
  find_library(IBVERBS_LIB ibverbs)
  find_library(RDMACM_LIB rdmacm)
  find_path(IBVERBS_INCLUDE infiniband/verbs.h)
  find_path(RDMACM_INCLUDE rdma/rdma_cma.h)
  if(NOT IBVERBS_LIB OR NOT RDMACM_LIB OR NOT IBVERBS_INCLUDE OR NOT RDMACM_INCLUDE)
    message(WARNING
      "libibverbs/librdmacm not found; building PeerCache C++ module in STUB mode. "
      "The pure-Python TCP fallback transport will be used at runtime.")
    set(PEERCACHE_NO_RDMA ON)
  endif()
endif()

set(PEERCACHE_SOURCES cpp/bindings/pybind.cpp)

if(NOT PEERCACHE_NO_RDMA)
  list(APPEND PEERCACHE_SOURCES
    cpp/src/rdma_context.cpp
    cpp/src/rdma_endpoint.cpp
    cpp/src/connection_manager.cpp
    cpp/src/transfer_engine.cpp)
endif()

pybind11_add_module(_peercache ${PEERCACHE_SOURCES})
target_include_directories(_peercache PRIVATE cpp/include)

if(PEERCACHE_NO_RDMA)
  target_compile_definitions(_peercache PRIVATE PEERCACHE_NO_RDMA=1)
else()
  target_include_directories(_peercache PRIVATE ${IBVERBS_INCLUDE} ${RDMACM_INCLUDE})
  target_link_libraries(_peercache PRIVATE ${IBVERBS_LIB} ${RDMACM_LIB} pthread)
endif()

install(TARGETS _peercache LIBRARY DESTINATION peercache)
