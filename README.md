# LaS-VPE Platform

[![AUR](https://img.shields.io/aur/license/yaourt.svg?maxAge=2592000)](LICENSE)

By Ken Yu, Yang Zhou, Da Li, Dangwei Li and Houjing Huang, under guidance of Dr. Zhang Zhang and Prof. Kaiqi Huang.

LaS-VPE Platform is a large-scale distributed video parsing and evaluation platform under the Intelligent Scene Exploration and Evaluation(iSEE) research platform of the Center for Research on Intelligent Perception and Computing(CRIPAC), Institute of Automation, Chinese Academy of Science. 

The platform is powered by Spark Streaming and Kafka.

The documentation is published on [Github Pages](https://kyu-sz.github.io/LaS-VPE-Platform).

## License

LaS-VPE Platform is released under the GPL License.

## Contents
1. [Requirements](#requirements)
2. [How to run](#how-to-run)
3. [How to monitor](#how-to-monitor)
4. [How to add a new module](#how-to-add-a-new-module)
5. [How to add a new native algorithm](#how-to-add-a-new-native-algorithm) 
6. [How to deploy a new version](#how-to-deploy-a-new-version)

## Requirements

1. Use Maven to build the project:

	```Shell
	sudo apt-get install maven
	```
	
2. Deploy Kafka(>=0.10.1.0), Spark(>=2.0.2), HDFS(>=2.7.2) and YARN(>=2.7.2) properly on your cluster.

Before you start your Kafka cluster, remember to configure the "max.request.size" to a large number, e.g. 157286400, so as to enable large message transferring through Kafka. This should be configured in "server.properties".

To enable multi-appications running concurrently, see [Job-Scheduling](https://spark.apache.org/docs/1.2.0/job-scheduling.html) and configure your environment.

## How to run

Clone the project to your cluster:

```Shell
# Make sure to clone with --recursive
git clone --recursive https://github.com/kyu-sz/LaS-VPE-Platform
```

Build and pack the system into a JAR:

```Shell
./sbin/build-native-libs.sh
mvn package
```

If your maven resolves dependencies at a low speed, try ```mvn -Dmaven.artifact.threads=100 package``` or add ```export MAVEN_OPTS=-Dmaven.artifact.threads=100``` to your ~/.bashrc.

Configure the environment and running properties in the files in [conf](conf).

Specially, modify the [cluster-env.sh](conf/cluster-env.sh) in [conf](conf) to adapt to your cluster address.

Upload the whole project to your cluster:

```Shell
./sbin/upload.sh
```

Then switch to the terminal of your cluster, change to the platform folder and deliver native libraries to worker nodes using [install.sh](sbin/install.sh) in [sbin](sbin) on your cluster. Note that this script requires the _HADOOP_HOME_ environment variable.

```Shell
./sbin/install.sh
```

Finally, you can start the applications by invoking the scripts in the home directory by command like "./sbin/run-*.sh".

It is recommended to last start the [run-command-generating-app.sh](sbin/run-command-generating-app.sh), which is the debugging tool to simulate commands to the message handling application.

Welcome to read Ken Yu's Chinese [blog](http://blog.csdn.net/kyu_115s/article/details/51887223) on experiences gained during the development.

## How to monitor

To briefly monitor, some informations are printed to the console that starts each module. However, to use this function, you must have the name of the host you start the module registered to each task nodes.

To fully monitor your Spark application, you might need to access the log files in the slave nodes. However, if your application runs on a cluster without desktop, and you connect remotely to the cluster, you might not be able to access the web pages loaded from the slave nodes.

To solve this problem, first add the address address of the slave nodes to the /etc/hosts in the master node. Make sure the master node can access the pages on slave nodes by terminal browsers like w3m or lynx. In Ubuntu, they can be installed by ```sudo apt-get install w3m``` or ```sudo apt-get install lynx```.

Then, configure your master node to be a proxy server using Squid. Tutors can be found in websites like [Help-Ubuntu-Squid](https://help.ubuntu.com/community/Squid).

Finally, configure your browser to use the proxy provided by you master node. Then it would be able to access pages on slave nodes.

In Firefox, it is recommended to use the AutoProxy plugin for enabling proxy. Beside obvious configurations, you need to first access *about:config*, then set *network.proxy.socks_remote_dns* as *true*.

## Basic conceptions in the project

_Application_: Same as that in YARN.

_Stream_: A flow of DStreams. Each stream may take in more than one input Kafka topic, but output at most one kind of output. An _Application_ may contains multiple streams.

_Node_: An execution of a _Stream_. A pack of input data and parameters are input into the stream.

_ExecutionPlan_: A flow graph of _Node_.

## How to add a new module

A new module may be based on some algorithms whose codes are written in other languages, so you first need to wrap them into JAVA using JNI.

See an application such as [PedestrianTrackingApp](src/main/java/org/cripac/isee/pedestrian/tracking/PedestrianTracker.java), etc. for example of how to write an application module. Write your own module then add it to this project. Also register its class name to the [AppManager](src/main/java/org/cripac/isee/vpe/ctrl/AppManager.java) by adding a line in the static block, similar to other lines.

You may also need to extend the [CommandGeneratingApp](src/main/java/org/cripac/isee/vpe/debug/CommandGeneratingApp.java), [MessageHandlingApp](src/main/java/org/cripac/isee/vpe/ctrl/MessageHandlingApp.java) and [DataManagingApp](src/main/java/org/cripac/isee/vpe/data/DataManagingApp.java) for support of the module.

## How to add a new native algorithm

You may want to run algorithms written in other languages like C/C++ on this platform. There are already examples of them: see [Video-Decoder](Video-Decoder) and [ISEE-Basic=Pedestrian-Tracker](ISEE-Basic=Pedestrian-Tracker). 

First of all, you should wrap your algorithm with JNI. It is recommended to implement this in another GitHub repository, and import it as a submodule.
 
Then, add the corresponding Java class to the platform. Be careful to put it in a suitable package.
 
Finally, build your algorithm project, and copy the resulting shared JNI library and those it depends on into the [library folder](lib/linux) directory.
  
To enable auto building and cleaning together with Maven, it is recommended to use CMake to build your project. Then edit the [native library building script](sbin/build-native-libs.sh) and [native library cleaning script](sbin/clean-native-libs.sh), following the examples in them.
 
If the new algorithm requires extra configuration files, remember to register them to the [CongigFileManager](src/main/java/org/cripac/isee/vpe/ctrl/ConfigFileManager.java).

## How to deploy a new version

Pack the new or modified project into a new JAR.

If you have updated the version number, remember to check the [system property file](conf/system.properties), where there is an option named "vpe.platform.jar" specifying the name of the JAR file to upload, and it should be the same as that of your newly built JAR file.

Upload the JAR to your cluster with your customized [uploading script](sbin/upload.sh).

After that, kill the particular old application and run the new one. It need not restart other modules! Now your module runs together with the original modules.

However, for modified modules, if you have run them once with checkpoint enabled, you should clean the old checkpoint directory or use a new one, so that the system can run new contexts rather than recovering old ones.

Sometimes you may also need to clean Kafka and Zookeeper logs, which are defaultly stored in the /tmp folder in each executor.