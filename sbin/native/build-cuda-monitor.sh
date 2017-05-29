#!/usr/bin/env bash

PROJECT_PATH=$(cd `dirname "${BASH_SOURCE[0]}"`/../..; pwd)
NATIVE_SRC=${PROJECT_PATH}/src/native

##################################################
mkdir -p ${PROJECT_PATH}/lib/x64 && \
##################################################
cd ${NATIVE_SRC}/CudaMonitor4j && \
##################################################
mkdir -p Release && cd Release && \
cmake -DCMAKE_BUILD_TYPE=Release .. && \
##################################################
make -j 16
if [ $? -ne 0 ]
then
  exit $?
fi
##################################################
cp -Rpu libCudaMonitor4j.so ${PROJECT_PATH}/lib/x64 || :
##################################################