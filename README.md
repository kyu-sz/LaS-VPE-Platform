# LaS-VPE Platform

[![License](https://img.shields.io/aur/license/yaourt.svg)](LICENSE)

LaS-VPE Platform is a large-scale distributed video parsing and evaluation platform under the Intelligent Scene Exploration and Evaluation(iSEE) research platform of the Center for Research on Intelligent Perception and Computing(CRIPAC). 

The platform is powered by Spark Streaming and Kafka.

It is mainly developed by Ken Yu and Yang Zhou.

# License

LaS-VPE Platform is released under the GPL License.

# How to run

The platform requires Kafka and Spark Streaming.

There are various scripts in the [sbin](sbin) directory to start parts of the platform respectively.

First start Kafka, HDFS, YARN and Spark properly.

To enable multi-appications running concurrently, see [Job-Scheduling](https://spark.apache.org/docs/1.2.0/job-scheduling.html) and configure your environment.

Then configure the platform through the [system.properties](system.properties) file (now you do not need to upload the file to HDFS even if the system is running on YARN).

Build and pack the system into a jar file in ./bin.

Invoke the scripts in the home directory by command like "./sbin/*.sh", after setting some params in it, for example, the path of the [system.properties](system.properties) file.

It is recommended to last start the [run-command-generating-app.sh](sbin/run-command-generating-app.sh), which is the debugging tool to simulate commands to the message handling application.

Welcome to read Ken Yu's Chinese [blog](http://blog.csdn.net/kyu_115s/article/details/51887223) on experiences gained during the development.

# How to monitor

To briefly monitor, some informations are printed to the console that starts each module. However, to use this function, you must have the name of the host you start the module registered to each task nodes.

To fully monitor your Spark application, you might need to access the log files in the slave nodes. However, if your application runs on a cluster without desktop, and you connect remotely to the cluster, you might not be able to access the web pages loaded from the slave nodes.

To solve this problem, first add the ip address of the slave nodes to the /etc/hosts in the master node. Make sure the master node can access the pages on slave nodes by terminal browsers like w3m or lynx. In Ubuntu, they can be installed by
```shell
sudo apt-get install w3m
```
or
```shell
sudo apt-get install lynx
```

Then, configure your master node to be a proxy server using Squid. Tutors can be found in websites like [Help-Ubuntu-Squid](https://help.ubuntu.com/community/Squid).

Finally, configure your browser to use the proxy provided by you master node. Then it would be able to access pages on slave nodes.

In Firefox, it is recommended to use the AutoProxy plugin for enabling proxy. Beside obvious configurations, you need to first access *about:config*, then set *network.proxy.socks_remote_dns* as *true*.

# How to add a new module

A new module may be based on some algorithms whose codes are written in other languages, so you first need to wrap them into JAVA using JNI.

See an application such as [PedestrianTrackingApp](src/org/casia/cripac/isee/pedestrian/tracking/PedestrianTracker.java), etc. for example of how to write an application module. Write your own module then add it to this project. Also register its class name to the [AppManager](src/org/casia/cripac/isee/vpe/ctrl/AppManager.java) by adding a line in the static block, similar to other lines.

You may also need to extend the [CommandGeneratingApp](src/org/casia/cripac/isee/vpe/debug/CommandGeneratingApp.java), [MessageHandlingApp](src/org/casia/cripac/isee/vpe/ctrl/MessageHandlingApp.java), [DataFeedingApp](src/org/casia/cripac/isee/vpe/data/DataFeedingApp.java) and [MetadataSavingApp](src/org/casia/cripac/isee/vpe/data/MetadataSavingApp.java) for support of the module.

# How to deploy a new version of the platform

Pack the new or modified project into a new JAR, upload it to your cluster, and start the particular module. It need not restart other modules! Now your module runs together with the original modules.

However, for modified modules, if you have run them once with checkpoint enabled, you should clean the old checkpoint directory or use a new one, so that the system can run new contexts rather than recovering old ones.

Sometimes you may also need to clean Kafka and Zookeeper logs, which are stored in the /tmp folder in each executor.