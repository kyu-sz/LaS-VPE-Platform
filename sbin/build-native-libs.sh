#!/usr/bin/env bash

echo "Building native libraries..."

##################################################
# Set up environment
##################################################
echo "Setting up environment..."
##################################################
PROJECT_PATH=$(cd `dirname "${BASH_SOURCE[0]}"`/..; pwd)
NATIVE_SRC=${PROJECT_PATH}/src/native
if [ $? -ne 0 ]
then
  exit $?
fi
##################################################
mkdir -p ${PROJECT_PATH}/lib/linux
if [ $? -ne 0 ]
then
  exit $?
fi
##################################################
mkdir -p ${PROJECT_PATH}/lib/windows
if [ $? -ne 0 ]
then
  exit $?
fi
##################################################

##################################################
# Build ISEE-Basic-Pedestrian-Tracker
##################################################
echo "Building ISEE-Basic-Pedestrian-Tracker..."
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

##################################################
# Build Caffe
##################################################
echo "Building Caffe..."
##################################################
cd ${NATIVE_SRC}/caffe
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
cp -Rpu .build_release/lib/libcaffe.so* ${PROJECT_PATH}/lib/linux || :
##################################################

echo "Successfully finished building native libraries!"