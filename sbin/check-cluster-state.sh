#!/usr/bin/env bash
. /etc/profile
ansible all -a "$JAVA_HOME/bin/jps"
