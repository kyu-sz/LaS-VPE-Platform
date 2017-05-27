# LaS-VPE Platform

[![AUR](https://img.shields.io/aur/license/yaourt.svg?maxAge=2592000)](LICENSE)

By Ken Yu, Yang Zhou, Da Li, Dangwei Li and Houjing Huang,
under guidance of Dr. Zhang Zhang and Prof. Kaiqi Huang.

LaS-VPE Platform is a large-scale distributed video parsing and evaluation
platform under the Intelligent Scene Exploration and Evaluation(iSEE) research
platform of the Center for Research on Intelligent Perception and
Computing(CRIPAC), Institute of Automation, Chinese Academy of Science. 

The platform is powered by Spark Streaming and Kafka.

The documentation is published on
[Github Pages](https://kyu-sz.github.io/LaS-VPE-Platform).

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

1. Use Maven to build the project.

    For Ubuntu:
	```bash
	sudo apt-get install maven
	```
	For CentOS:
	```bash
	sudo yum install maven
	```
	
2. Increase open files limit in Linux.

    Paste following towards end of _/etc/security/limits.conf_:
    
    ```bash
       *       hard    nofile  500000
       *       soft    nofile  500000
       root    hard    nofile  500000
       root    soft    nofile  500000
    ```
	
3. Deploy Kafka(>=0.10.2.0), Spark(>=2.0.2), HDFS(>=2.7.2) and YARN(>=2.7.2)
properly on your cluster.

    * To enable multi-appications running concurrently, see
      [Job-Scheduling](https://spark.apache.org/docs/1.2.0/job-scheduling.html)
      and configure your environment.
      
    * Disable virtual memory checking of YARN in "yarn-site.xml" to enable GPU applications:
      ```xml
      <property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
      </property>
      ```

4. If you choose to run algorithms based on __Caffe__ or __Caffe2__,
   no matter the algorithms use CPU-only or GPUs,
   you must have __CUDA 8.0__ installed on __all nodes__ in your cluster.

## How to run

1. Clone the project to your cluster:

    ```bash
    # Make sure to clone with --recursive
    git clone --recursive https://github.com/kyu-sz/LaS-VPE-Platform
    ```
    
2. Retrieve additional model files such as _DeepMAR.caffemodel_ from algorithm providers.
Put them in appropriate locations. For example, the _DeepMAR.caffemodel_ should be put in
_${PROJECT_DIR}/models/DeepMAR/_. See [models](models).

3. Configure the environment and running properties in the files in [conf](conf).
 Specially, modify the [cluster-env.sh](conf/cluster-env.sh) in [conf](conf)
to adapt to your cluster address.

4. Build and pack the system into a JAR:

    ```bash
    mvn package
    ```

    * Tests are enabled by default. Use ```mvn package -Dmaven.test.skip.exec```
      to skip them when your machine is not able to run the tests or you are
      sure the tests will pass and need to save time.
      
    * If your maven resolves dependencies at a low speed, try
    ```mvn -Dmaven.artifact.threads=100 package``` or add
    ```export MAVEN_OPTS=-Dmaven.artifact.threads=100``` to your ~/.bashrc.
    
    * You may also try a Maven mirror. For users in China, the Aliyun mirror is recommended.

5. Install all the dependencies required by the native libraries on __all nodes__ in your cluster.
      
    Especially, we use OpenBLAS for Caffe2 by default for best efficiency. Both Eigen3 and OpenBLAS should be installed
    by package management tools like yum (CentOS) or apt (Ubuntu) in addition to the packages listed on Caffe2's
    installation guide:
    
    For CentOS:
    
    ```bash
    sudo yum install openblas-devel eigen3-devel
    sudo ln -s /usr/lib64/libopenblas.so /usr/lib64/libcblas.so
    ```
    
    For Ubuntu:
    
    ```bash
    sudo apt install openblas-dev eigen3-dev
    sudo ln -s /usr/lib/libopenblas.so /usr/lib/libcblas.so
    ```
    
    Note that on CentOS, the libgflags-devel is too old for the latest GLog. It should be compiled from the latest
    version on [GitHub](https://github.com/gflags/gflags). However, these libraries might cause compilation failure
    of Caffe in old versions. Therefore, you need not install them on every node, since these nodes might be shared by
    other users who might use old Caffe. The [Caffe2 installing script](sbin/native/build-caffe2.sh) can automatically
    copy the libraries to the project library folder, so that they can be distributed to the Hadoop native directories.
    
    Use the following commands to install gflags and glog (using a sudoer account):
    
    ```bash
    git clone https://github.com/gflags/gflags.git && \
    cd gflags && \
    mkdir build && cd build && \
    cmake3 -DBUILD_SHARED_LIBS=ON -DCMAKE_CXX_FLAGS='-fPIC' .. && \
    make -j 8 && sudo make install && cd ../.. && \
    git clone https://github.com/google/glog && \
    cd glog && \
    mkdir build && cd build && \
    cmake3 -DCMAKE_CXX_FLAGS='-fPIC' .. && \
    make -j 8 && sudo make install && cd ../..
    ```

6. Build and deliver the native libraries to worker nodes using
 [install.sh](sbin/install.sh) in [sbin](sbin).
     
     ```bash
     ./sbin/install.sh
     ```
        
     Note that this script requires the _HADOOP_HOME_ environment variable.
     
     * Trouble shooting: 
     
        * libcblas.so is not found (-lcblas fails):
        
            Create a link from the OpenBLAS library:
            
            ```bash
            sudo ln -s /usr/lib64/libopenblas.so /usr/lib64/libcblas.so
            ```
            
        * Built once and failed, then reconfigure the environment and dependencies but still fails:
        
            Call the [cleaning script](sbin/clean-native-libs.sh), then build again.
            
        * Eigen/Core not found:
        
            Ensure you have installed Eigen3 on your computer and its headers are in "/usr/include" or
            "/usr/local/include".
            
            For CentOS, use ```yum install eigen3-devel```.

7. Finally, you can start the applications by invoking the scripts in the home
   directory by command like "./sbin/run-*.sh".

    * It is recommended to last start the
      [run-command-generating-app.sh](sbin/run-command-generating-app.sh), which is
      the debugging tool to simulate commands to the message handling application.

Welcome to Ken Yu's Chinese
[blog](http://blog.csdn.net/kyu_115s/article/details/51887223) on experiences
gained during the development.

## How to monitor

To briefly monitor, some informations are printed to the console that starts
each module. However, to use this function, the console terminal must be able
to access the Kafka cluster.

To fully monitor your Spark application, you might need to access the log files
in the slave nodes. However, if your application runs on a cluster without
desktop, and you connect remotely to the cluster, you might not be able to
access the web pages loaded from the slave nodes.

To solve this problem, first add the address address of the slave nodes to the
/etc/hosts in the master node. Make sure the master node can access the pages
on slave nodes by terminal browsers like w3m or lynx. In Ubuntu, they can be
installed by ```sudo apt-get install w3m``` or ```sudo apt-get install lynx```.

Then, configure your master node to be a proxy server using Squid. Tutorials
can be found in websites like
[Help-Ubuntu-Squid](https://help.ubuntu.com/community/Squid).

Finally, configure your browser to use the proxy provided by you master node.
Then it would be able to access pages on slave nodes.

For Firefox, it is recommended to use the AutoProxy plugin for enabling proxy.
Beside obvious configurations, you need to first access *about:config*, then
set *network.proxy.socks_remote_dns* as *true*.

## Basic concepts in this project

* _Application_: Same as that in YARN.

* _Stream_: A flow of DStreams. Each stream may take in more than one kind of data,
 but outputs at most one kind of data. An _Application_ may contains multiple streams.

* _Node_: An execution of a _Stream_. A pack of input data and parameters are
input into the stream.

* _ExecutionPlan_: A flow graph of _Node_.

## How to add a new module

Before starting your work on developing this platform, choosing a suitable IDE
will definitely improve your efficiency! Here we strongly recommend the
Intellij IDEA, since our project contains a mixture of Java codes and Scala
codes, while is organized with Maven. Intellij IDEA is a much better choice than
Eclipse.

1. See an application such as
[PedestrianTrackingApp](src/main/java/org/cripac/isee/pedestrian/tracking/PedestrianTracker.java) 
 for example of how to write an application module.
 
2. Write your own module then add it to this project.

3. Register its class name to the
 [AppManager](src/main/java/org/cripac/isee/vpe/ctrl/AppManager.java)
  by adding a line in the static block, similar to the existing lines.

4. Extend the
[CommandGeneratingApp](src/main/java/org/cripac/isee/vpe/debug/CommandGeneratingApp.java),
[MessageHandlingApp](src/main/java/org/cripac/isee/vpe/ctrl/MessageHandlingApp.java)
and [DataManagingApp](src/main/java/org/cripac/isee/vpe/data/DataManagingApp.java)
for support of the module.

## How to add a new native algorithm

You may want to run algorithms written in other languages like C/C++ on this
platform. Here is an example:
 [ISEE-Basic-Pedestrian-Tracker](src/native/ISEE-Basic-Pedestrian-Tracker). 

1. Wrap your algorithm with JNI. It is recommended to
implement this in another GitHub repository, and import it as a submodule.
 
2. Add the corresponding Java class to the platform. Be careful to put
it in a suitable package.
 
3. Build your algorithm project, and copy the resulting shared JNI
library and those it depends on into the [library folder](lib/x64) directory.
  
    * To enable auto building and cleaning together with Maven, it is recommended to
      use CMake to build your project. Then edit the
      [native library building script](sbin/build-native-libs.sh) and
      [native library cleaning script](sbin/clean-native-libs.sh), following the
      examples in them.
 
4. If the new algorithm requires extra configuration files, register them to the
[CongigFileManager](src/main/java/org/cripac/isee/vpe/ctrl/ConfigFileManager.java).

## How to deploy a new version of an application

1. Kill the particular old application.

2. If checkpointing is enabled, clean the old checkpoint directory for that application or use a new one.

3. Build the new version of project using ```mvn package -Dmaven.test.skip.exec```.

4. Run the new application with a script. You do not need to stop other applications~