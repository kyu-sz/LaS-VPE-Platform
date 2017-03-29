#!/usr/bin/env bash

PROJECT_PATH=$(cd `dirname "${BASH_SOURCE[0]}"`/../..; pwd)
NATIVE_SRC=${PROJECT_PATH}/src/native
mkdir -p ${PROJECT_PATH}/lib/linux
mkdir -p ${PROJECT_PATH}/lib/windows

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
make clean
make -j 16
if [ $? -ne 0 ]
then
  exit $?
fi
##################################################
cp -Rpu lib/libDeepMAR.so ${PROJECT_PATH}/lib/linux || :
cp -Rpu lib/jni/libDeepMAR_caffe_jni.so ${PROJECT_PATH}/lib/linux || :
##################################################