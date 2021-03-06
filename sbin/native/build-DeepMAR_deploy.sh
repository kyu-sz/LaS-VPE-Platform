#!/usr/bin/env bash

PROJECT_PATH=$(cd `dirname "${BASH_SOURCE[0]}"`/../..; pwd)
NATIVE_SRC=${PROJECT_PATH}/src/native

##################################################
mkdir -p ${PROJECT_PATH}/lib/x64 && \
##################################################
cd ${NATIVE_SRC}/DeepMAR_deploy && \
##################################################
mkdir -p Release && cd Release && \
cmake -DCMAKE_BUILD_TYPE=Release -DCAFFE2_INSTALL_HOME=${CAFFE2_INSTALL_HOME} .. && \
##################################################
make -j 16
if [ $? -ne 0 ]
then
  exit $?
fi
##################################################
cp -Rpu lib/libDeepMARCaffe2.so ${PROJECT_PATH}/lib/x64 || :
cp -Rpu lib/jni/libjniDeepMARCaffe2.so ${PROJECT_PATH}/lib/x64 || :
##################################################
