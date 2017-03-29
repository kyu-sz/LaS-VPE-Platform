#!/usr/bin/env bash

##################################################
PROJECT_PATH=$(cd `dirname "${BASH_SOURCE[0]}"`/../..; pwd)
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