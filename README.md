# VPE-Platform

[![License](https://img.shields.io/aur/license/yaourt.svg)](LICENSE)

VPE-Platform is a video parsing and evaluation platform under the Intelligent Scene Exploration and Evaluation(iSEE) research platform of the Center for Research on Intelligent Perception and Computing(CRIPAC). 

The platform is powered by Spark Streaming and Kafka.

It is mainly developed by Ken Yu and Yang Zhou.

# License

VPE-Platform is released under the GPL License.

# How to run

The VPE-Platform requires Kafka and Spark Streaming.

There are various scripts in the [sbin](sbin) directory to start parts of the platform respectively.

First start Kafka, HDFS, YARN and Spark properly.
Then configure the platform through the [system.properties](system.properties) file, and upload the file to HDFS if the system is running on YARN.

Build and pack the system into a jar file in ./bin.

Invoke the scripts in the home directory by command like "./sbin/*.sh", after setting some params in it, for example, the path of the [system.properties](system.properties) file.

It is recommended to last start the [run-command-generating-app.sh](sbin/run-command-generating-app.sh), which is the debugging tool to simulate commands to the message handling application.
