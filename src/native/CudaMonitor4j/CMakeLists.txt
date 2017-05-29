cmake_minimum_required(VERSION 2.8.7)
project(CudaMonitor4j)

set(CMAKE_CXX_STANDARD 11)

set(CUDA_HOME $ENV{CUDA_HOME} CACHE PATH "Home directory of Cuda.")
if (${CUDA_HOME} STREQUAL "")
    message(FATAL_ERROR "CUDA_HOME not set!")
endif ()
link_directories(${CUDA_HOME}/lib64 ${CUDA_HOME}/lib64/stubs)

set(SOURCE_FILES cuda_monitor4j.cpp)
add_library(CudaMonitor4j SHARED ${SOURCE_FILES})

set(JAVA_HOME $ENV{JAVA_HOME} CACHE PATH "Home directory of Java Divelooment Toolkit (JDK).")
message(STATUS "Using JNI at ${JAVA_HOME}/include")
target_include_directories(CudaMonitor4j PUBLIC ${JAVA_HOME}/include)
if (MSVC)
    target_include_directories(CudaMonitor4j PUBLIC ${JAVA_HOME}/include/windows)
else ()
    target_include_directories(CudaMonitor4j PUBLIC ${JAVA_HOME}/include/linux)
endif ()

message(STATUS "Using CUDA at ${CUDA_HOME}")
target_include_directories(CudaMonitor4j PUBLIC ${CUDA_HOME}/include)
target_link_libraries(CudaMonitor4j nvidia-ml)