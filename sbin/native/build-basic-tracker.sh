#!/usr/bin/env bash

PROJECT_PATH=$(cd `dirname "${BASH_SOURCE[0]}"`/../..; pwd)
NATIVE_SRC=${PROJECT_PATH}/src/native
mkdir -p ${PROJECT_PATH}/lib/x64

##################################################
cd ${NATIVE_SRC}/ISEE-Basic-Pedestrian-Tracker
if [ $? -ne 0 ]
then
  exit $?
fi
##################################################
mkdir -p Release
cd Release
cmake -DCMAKE_BUILD_TYPE=Release ..
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
cp -Rpu ../lib/libbasic_pedestrian_tracker.so ${PROJECT_PATH}/lib/x64 || :
cp -Rpu lib/jni/libjnibasic_pedestrian_tracker.so ${PROJECT_PATH}/lib/x64 || :
##################################################