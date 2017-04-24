#!/usr/bin/env bash
projectpath=$(cd `dirname "${BASH_SOURCE[0]}"`/..; pwd)

# Clean ISEE-Basic-Pedestrian-Tracker
rm ${projectpath}/lib/x64/libbasic_pedestrian_tracker.so
rm ${projectpath}/lib/x64/libjnibasic_pedestrian_tracker.so
cd ${projectpath}/src/native/ISEE-Basic-Pedestrian-Tracker/Release
make clean

# Clean Caffe
rm ${projectpath}/lib/x64/libcaffe.so*
cd ${projectpath}/src/native/caffe/
make clean

# Clean DeepMAR_deploy
rm ${projectpath}/lib/x64/libDeepMARCaffe.so
rm ${projectpath}/lib/x64/libjniDeepMARCaffe.so
cd ${projectpath}/src/native/DeepMAR_deploy/Release
make clean