#!/usr/bin/env bash
cd $(dirname "${BASH_SOURCE[0]}")/../
source conf/cluster-env.sh
ssh ${DRIVER_USER}@${DRIVER_NODE} "mkdir -p ${VPE_FOLDER}/src/"
#scp bin/las-vpe-platform-*-full.jar ${DRIVER_USER}@${DRIVER_NODE}:${VPE_FOLDER}
#scp -r sbin ${DRIVER_USER}@${DRIVER_NODE}:${VPE_FOLDER}
#scp -r conf ${DRIVER_USER}@${DRIVER_NODE}:${VPE_FOLDER}
#scp -r src/native ${DRIVER_USER}@${DRIVER_NODE}:${VPE_FOLDER}/src/

rsync -rav -e ssh --include '*/' \
 --include='*.sh' --include='*.jar' --include='*.so' --include='*.dll' --include='*.lib'\
 --include='*.conf' --include='*.properties' --include='*.xml' --include='*.conf' \
 --include='*.cpp' --include='*.hpp' --include='*.c' --include='*.h' --include='*.txt' \
 --exclude='*' --exclude='CMakeCache.txt'\
 . \
 ${DRIVER_USER}@${DRIVER_NODE}:${VPE_FOLDER}