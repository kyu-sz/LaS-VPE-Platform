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

To enable multi-appications running concurrently, see [Job-Scheduling](https://spark.apache.org/docs/1.2.0/job-scheduling.html) and configure your environment.

Then configure the platform through the [system.properties](system.properties) file (now you do not need to upload the file to HDFS even if the system is running on YARN).

Build and pack the system into a jar file in ./bin.

Invoke the scripts in the home directory by command like "./sbin/*.sh", after setting some params in it, for example, the path of the [system.properties](system.properties) file.

It is recommended to last start the [run-command-generating-app.sh](sbin/run-command-generating-app.sh), which is the debugging tool to simulate commands to the message handling application.

Welcome to read Ken Yu's Chinese [blog](http://blog.csdn.net/kyu_115s/article/details/51887223) on experiences gained during the development.

# How to monitor

To monitor your Spark application, you might need to access the log files in the slave nodes. However, if your application runs on a cluster without desktop, and you connect remotely to the cluster, you might not be able to access the web pages loaded from the slave nodes.

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

In Firefox, it is recommended to use the AutoProxy plugin for enabling proxy. Beside obvious configurations, you need to first access [about:config](about:config), then set network.proxy.socks_remote_dns as "true".
