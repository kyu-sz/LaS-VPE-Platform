#!/usr/bin/env bash

##################################################
source `dirname "${BASH_SOURCE[0]}"`/env.sh
if [ $? -ne 0 ]
then
  exit $?
fi
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