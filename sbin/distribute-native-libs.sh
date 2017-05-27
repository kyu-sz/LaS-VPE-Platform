#!/usr/bin/env bash

PROJECT_PATH=$(cd `dirname "${BASH_SOURCE[0]}"`/..; pwd)

source ${PROJECT_PATH}/conf/cluster-env.sh

cd ${PROJECT_PATH}

for SLAVE in `cat ${HADOOP_HOME}/etc/hadoop/slaves`
do
    echo 'Installing native libraries to' ${SLAVE}
    scp -r ./lib/x64/*.so* ${USER}@${SLAVE}:${SLAVE_HADOOP_HOME}/lib/native
done