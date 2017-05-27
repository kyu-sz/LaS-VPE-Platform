#!/usr/bin/env bash

function check_env() {
    _ENV=$1
    _PATH=$2
    result=$(printenv ${_ENV} | grep "${_PATH}")
    if [[ "$result" != "" ]]
    then
        echo "${_ENV} contains ${_PATH}"
    else
        echo "${_ENV} does not contain ${_PATH}! Adding in ~/.bashrc"
        echo "" >> ~/.bashrc"
        echo "${_ENV}=\$${_ENV}:${_PATH}" >> ~/.bashrc"
    fi
    return 0
}

PROJECT_PATH=$(cd `dirname "${BASH_SOURCE[0]}"`/..; pwd)
NATIVE_SRC=${PROJECT_PATH}/src/native

source ${PROJECT_PATH}/conf/cluster-env.sh

CAFFE2_INSTALL_HOME=/home/${USER}

# Build ISEE-Basic-Pedestrian-Tracker
echo "Building ISEE-Basic-Pedestrian-Tracker..."
source ${PROJECT_PATH}/sbin/native/build-basic-tracker.sh

# Build Caffe
echo "Building Caffe2..."
source ${PROJECT_PATH}/sbin/native/build-caffe2.sh
export CAFFE2_HOME=${NATIVE_SRC}/caffe2

# Build DeepMAR_deploy
echo "Building DeepMAR_deploy..."
source ${PROJECT_PATH}/sbin/native/build-DeepMAR_deploy.sh

echo "Successfully finished building native libraries!"
