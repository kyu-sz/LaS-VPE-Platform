# Configuration Directory

This directory stores all the configuration files.

## System-wide configurations

There are several system-wide configuration files:

* cluster-env.sh:       Environment parameters.

* log4j.properties:     Properties for Log4j. This will be uploaded to YARN.

* spark-defaults.conf:  Default configurations of Spark.

* vpe-scheduler.xml:    Configuration of the VPE scheduler for YARN.

* system.properties:    Other configurations for the LaS-VPE Platform.
                            It is directly read and analysed by the codes in the project.
                            It also contains some configurations that can overwrite the Spark configurations.
                        
## Application configurations
 
Configurations for one application should be stored in a folder with the same name as the application.

Application configurations, stored in _app.properties_ can supplement or overwrite system-wide configurations.

There can be other configuration files, such as configurations for the pedestrian tracker.