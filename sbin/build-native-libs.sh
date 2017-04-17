#!/usr/bin/env bash

PROJECT_PATH=$(cd `dirname "${BASH_SOURCE[0]}"`/..; pwd)
NATIVE_SRC=${PROJECT_PATH}/src/native

# Build ISEE-Basic-Pedestrian-Tracker
echo "Building ISEE-Basic-Pedestrian-Tracker..."
source ${PROJECT_PATH}/sbin/native/build-basic-tracker.sh

# Build Caffe
echo "Building Caffe..."
source ${PROJECT_PATH}/sbin/native/build-caffe.sh
export CAFFE_HOME=${NATIVE_SRC}/caffe

# Build DeepMAR_deploy
echo "Building DeepMAR_deploy..."
source ${PROJECT_PATH}/sbin/native/build-DeepMAR_deploy.sh

echo "Successfully finished building native libraries!"
