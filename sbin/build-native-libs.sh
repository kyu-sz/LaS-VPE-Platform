#!/usr/bin/env bash

# Build ISEE-Basic-Pedestrian-Tracker
echo "Building ISEE-Basic-Pedestrian-Tracker..."
source ${PROJECT_PATH}/sbin/native/build-basic-tracker.sh

# Build Caffe
echo "Building Caffe..."
source ${PROJECT_PATH}/sbin/native/build-caffe.sh

# Build DeepMAR_deploy
echo "Building DeepMAR_deploy..."
source ${PROJECT_PATH}/sbin/native/build-DeepMAR_deploy.sh

echo "Successfully finished building native libraries!"