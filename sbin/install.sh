#!/usr/bin/env bash

PROJECT_PATH=$(cd `dirname "${BASH_SOURCE[0]}"`/..; pwd)

source ${PROJECT_PATH}/sbin/build-native-libs.sh
source ${PROJECT_PATH}/sbin/distribute-native-libs.sh