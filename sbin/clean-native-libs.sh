#!/usr/bin/env bash
PROJECT_PATH=$(cd `dirname "${BASH_SOURCE[0]}"`/..; pwd)

# Clean ISEE-Basic-Pedestrian-Tracker
rm ${PROJECT_PATH}/lib/x64/libbasic_pedestrian_tracker.so
rm ${PROJECT_PATH}/lib/x64/libjnibasic_pedestrian_tracker.so
cd ${PROJECT_PATH}/src/native/ISEE-Basic-Pedestrian-Tracker/Release
make clean

# Clean Caffe
rm ${PROJECT_PATH}/lib/x64/libCaffe2_*PU.so
cd ${PROJECT_PATH}/src/native/caffe2/
make clean

# Clean DeepMAR_deploy
rm ${PROJECT_PATH}/lib/x64/libDeepMARCaffe2.so
rm ${PROJECT_PATH}/lib/x64/libjniDeepMARCaffe2.so
cd ${PROJECT_PATH}/src/native/DeepMAR_deploy/Release
make clean