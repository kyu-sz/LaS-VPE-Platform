#!/usr/bin/env bash

PROJECT_PATH=$(cd `dirname "${BASH_SOURCE[0]}"`/../..; pwd)
NATIVE_SRC=${PROJECT_PATH}/src/native
mkdir -p ${PROJECT_PATH}/lib/x64

##################################################
cd ${NATIVE_SRC}/DeepMAR_deploy
if [ $? -ne 0 ]
then
  exit $?
fi
##################################################
cmake .
if [ $? -ne 0 ]
then
  exit $?
fi
##################################################
make -j 16
if [ $? -ne 0 ]
then
  exit $?
fi
##################################################
cp -Rpu lib/libDeepMARCaffe.so ${PROJECT_PATH}/lib/x64 || :
cp -Rpu lib/jni/libjniDeepMARCaffe.so ${PROJECT_PATH}/lib/x64 || :
##################################################