#!/usr/bin/env bash
projectpath=$(cd `dirname "${BASH_SOURCE[0]}"`/..; pwd)

# Clean ISEE-Basic-Pedestrian-Tracker
rm ${projectpath}/lib/x64/libbasic_pedestrian_tracker.so
rm ${projectpath}/lib/x64/libbasic_pedestrian_tracker_jni.so
cd ${projectpath}/ISEE-Basic-Pedestrian-Tracker
make clean

# Clean Caffe
rm ${projectpath}/lib/x64/libcaffe.so*
cd ${projectpath}/caffe
make clean

# Clean DeepMAR_deploy
rm ${projectpath}/lib/x64/libDeepMAR.so
rm ${projectpath}/lib/x64/libDeepMAR_caffe_jni.so
cd ${projectpath}/DeepMAR_deploy
make clean