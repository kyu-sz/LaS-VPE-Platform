#!/usr/bin/env bash

##################################################
source `dirname "${BASH_SOURCE[0]}"`/env.sh
if [ $? -ne 0 ]
then
  exit $?
fi
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