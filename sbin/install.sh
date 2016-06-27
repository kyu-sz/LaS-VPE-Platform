#!/usr/bin/env bash

PROJECT_PATH=$(cd `dirname "${BASH_SOURCE[0]}"`/..; pwd)

source ${PROJECT_PATH}/conf/cluster-env.sh
source ${PROJECT_PATH}/sbin/build-native-libs.sh

cd ${PROJECT_PATH}

for SLAVE in `cat ${HADOOP_HOME}/etc/hadoop/slaves`
do
    echo 'Installing native libraries to' ${SLAVE}
    #ssh ${SLAVE}  "rm -r /tmp/lib" 2 >> /dev/null
    #scp -r lib/linux ${SLAVE}:/tmp/lib
    #ssh ${SLAVE} "sudo mv /tmp/lib/* /usr/local/lib/ && rm -r /tmp/lib"
    scp -r ./lib/linux/*.so ${SLAVE}:${SLAVE_HADOOP_HOME}/lib/native
done