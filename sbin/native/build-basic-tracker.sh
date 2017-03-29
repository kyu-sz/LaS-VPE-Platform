#!/usr/bin/env bash

PROJECT_PATH=$(cd `dirname "${BASH_SOURCE[0]}"`/../..; pwd)
NATIVE_SRC=${PROJECT_PATH}/src/native
mkdir -p ${PROJECT_PATH}/lib/linux
mkdir -p ${PROJECT_PATH}/lib/windows

##################################################
cd ${NATIVE_SRC}/ISEE-Basic-Pedestrian-Tracker
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
cp -Rpu lib/libbasic_pedestrian_tracker.so ${PROJECT_PATH}/lib/linux || :
cp -Rpu lib/jni/libbasic_pedestrian_tracker_jni.so ${PROJECT_PATH}/lib/linux || :
##################################################