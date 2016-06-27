#!/usr/bin/env bash
projectpath=$(cd `dirname "${BASH_SOURCE[0]}"`/..; pwd)

# Clean Video-Decoder
rm ${projectpath}/lib/linux/libvideo_decoder_jni.so || :
rm ${projectpath}/lib/windows/libvideo_decoder_jni.dll || :
cd ${projectpath}/Video-Decoder
make clean

# Clean ISEE-Basic-Pedestrian-Tracker
rm ${projectpath}/lib/linux/libbasic_pedestrian_tracker.so
rm ${projectpath}/lib/linux/libbasic_pedestrian_tracker_jni.so
cd ${projectpath}/ISEE-Basic-Pedestrian-Tracker
make clean