scp -r bin labadmin@rman-nod1:/home/labadmin/vpe-platform
scp -r sbin labadmin@rman-nod1:/home/labadmin/vpe-platform
scp log4j.properties labadmin@rman-nod1:/home/labadmin/vpe-platform
# At first time you upload this platform to your cluster, uncomment the following command.
# Modify the system property file uploaded to fit the cluster's environment.
# Then comment this command to avoid modification.
# scp system.properties labadmin@rman-nod1:/home/labadmin/vpe-platform