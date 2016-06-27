#!/usr/bin/env bash
. /etc/profile
ansible tasknodes -a "$JAVA_HOME/bin/jps"
