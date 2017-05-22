#!/usr/bin/env bash

PROJECT_PATH=$(cd `dirname "${BASH_SOURCE[0]}"`/../..; pwd)
NATIVE_SRC=${PROJECT_PATH}/src/native

##################################################
mkdir -p ${PROJECT_PATH}/lib/x64 && \
##################################################
cd ${NATIVE_SRC}/caffe2 && \
##################################################
echo "Installing Caffe2 to ${CAFFE2_INSTALL_HOME}"
mkdir -p build && cd build && \
mkdir -p ${CAFFE2_INSTALL_HOME}/include ${CAFFE2_INSTALL_HOME}/lib ${CAFFE2_INSTALL_HOME}/lib64 && \
cmake -DBLAS=OpenBLAS -DCMAKE_INSTALL_PREFIX=${CAFFE2_INSTALL_HOME} .. $(python ../scripts/get_python_cmake_flags.py) && \
make -j 16 && make install
if [ $? -ne 0 ]
then
  exit $?
fi
cp -Rpu ${CAFFE2_INSTALL_HOME}/lib/libCaffe2_CPU.so ${PROJECT_PATH}/lib/x64 || :
cp -Rpu ${CAFFE2_INSTALL_HOME}/lib/libCaffe2_GPU.so ${PROJECT_PATH}/lib/x64 || :
##################################################
# Here we need the NCCL dynamic libraries installed because for the current version of Caffe2,
# we find it not able to link symbols from the NCCL static library.
echo "Installing NCCL to ${CAFFE2_INSTALL_HOME}..."
cd ${NATIVE_SRC}/caffe2/third_party/nccl && \
make PREFIX=${CAFFE2_INSTALL_HOME} install -j 32
if [ $? -ne 0 ]
then
  exit $?
fi
cp -Rpu ${CAFFE2_INSTALL_HOME}/lib/libnccl.so* ${PROJECT_PATH}/lib/x64 || : 
##################################################
python -c 'from caffe2.python import core' 2>/dev/null && echo "Success" || echo "Failure"
check_env "PYTHONPATH" "${CAFFE2_INSTALL_HOME}"
check_env "PYTHONPATH" "${NATIVE_SRC}/caffe2/build"
check_env "LIBRARY_PATH" "${CAFFE2_INSTALL_HOME}/lib"
##################################################
