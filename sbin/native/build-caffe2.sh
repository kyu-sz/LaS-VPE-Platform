#!/usr/bin/env bash

PROJECT_PATH=$(cd `dirname "${BASH_SOURCE[0]}"`/../..; pwd)
NATIVE_SRC=${PROJECT_PATH}/src/native
mkdir -p ${PROJECT_PATH}/lib/x64

##################################################
cd ${NATIVE_SRC}/caffe2
if [ $? -ne 0 ]
then
  exit $?
fi
##################################################
make -j 16 && cd build && sudo make install
if [ $? -ne 0 ]
then
  exit $?
fi
cp -Rpu /usr/local/lib/libCaffe2_CPU.so ${PROJECT_PATH}/lib/x64 || :
cp -Rpu /usr/local/lib/libCaffe2_GPU.so ${PROJECT_PATH}/lib/x64 || :
##################################################
python -c 'from caffe2.python import core' 2>/dev/null && echo "Success" || echo "Failure"
check_env "PYTHONPATH" "/usr/local"
check_env "PYTHONPATH" ${NATIVE_SRC}/caffe2/build
check_env "LIBRARY_PATH" "/usr/local/lib"
##################################################