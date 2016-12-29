/***********************************************************************
 * This file is part of LaS-VPE Platform.
 *
 * LaS-VPE Platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * LaS-VPE Platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with LaS-VPE Platform.  If not, see <http://www.gnu.org/licenses/>.
 ************************************************************************/

package org.cripac.isee.vpe.ctrl;

import org.apache.commons.cli.*;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Level;
import org.apache.spark.launcher.SparkLauncher;
import org.cripac.isee.vpe.util.hdfs.HadoopHelper;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;
import org.xml.sax.SAXException;

import javax.annotation.Nonnull;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.Map.Entry;

/**
 * The SystemPropertyCenter class is responsible of managing the properties of
 * the systems. There are some properties predefined, and they can be
 * overwritten by command options or an extern property file. It can also
 * generate back command options for uses like SparkSubmit.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class SystemPropertyCenter implements Serializable {

    private static final long serialVersionUID = -6642856932636724919L;
    // Logger for parsing.
    private transient Logger logger = new ConsoleLogger(Level.INFO);

    // Zookeeper properties
    public String zkConn = "localhost:2181";
    public int sessionTimeoutMs = 10 * 10000;
    public int connectionTimeoutMs = 8 * 1000;
    // Kafka properties
    public String kafkaBootstrapServers = "localhost:9092";
    public int kafkaNumPartitions = 1;
    public int kafkaReplFactor = 1;
    public int kafkaMsgMaxBytes = 100000000;
    public int kafkaMaxRequestSize = 100000000;
    // Spark properties
    public String checkpointRootDir = "checkpoint";
    public String metadataDir = "metadata";
    public String sparkMaster = "local[*]";
    public String sparkDeployMode = "client";
    public String[] appsToStart = null;
    /**
     * Memory per executor (e.g. 1000M, 2G) (Default: 1G)
     */
    public String executorMem = "1G";
    /**
     * Number of executors to run (Default: 2)
     */
    public int numExecutors = 2;
    /**
     * Number of cores per executor (Default: 1)
     */
    public int executorCores = 1;
    /**
     * Total cores for all executors (Spark standalone and Mesos only).
     */
    public int totalExecutorCores = 1;
    /**
     * Memory for driver (e.g. 1000M, 2G) (Default: 1024 Mb)
     */
    public String driverMem = "1G";
    /**
     * Number of cores used by the driver (Default: 1)
     */
    public int driverCores = 1;
    /**
     * The hadoop queue to use for allocation requests (Default: 'default')
     */
    public String hadoopQueue = "default";
    public String sysPropFilePath = "conf/system.properties";
    /**
     * Application-specific property file. Properties loaded from it
     * will override those loaded from the system property file.
     * Leaving it as null will let the system automatically find
     * that in default places according to the application specified.
     */
    public String appPropFilePath = null;
    public String sparkConfFilePath = ConfManager.CONF_DIR + "/spark-defaults.conf";
    public String log4jPropFilePath = ConfManager.CONF_DIR + "/log4j.properties";
    public String hdfsDefaultName = "localhost:9000";
    public String yarnResourceManagerHostname = "localhost";
    public String jarPath = "bin/vpe-platform.jar";
    /**
     * Number of parallel streams pulling messages from Kafka Brokers.
     */
    public int numRecvStreams = 5;
    /**
     * Duration for buffering results.
     */
    public int bufDuration = 600000;
    /**
     * Estimated time in milliseconds to be consumed in the process of each RDD.
     */
    public int procTime = 10000;
    /**
     * Whether to print verbose running information.
     */
    public boolean verbose = false;

    /**
     * Subclasses can continue to analyze this property storage.
     */
    protected Properties sysProps = new Properties();

    /**
     * Construction function supporting allocating a SystemPropertyCenter then
     * filling in the properties manually.
     */
    public SystemPropertyCenter() throws SAXException, ParserConfigurationException, URISyntaxException {
        this(new String[0]);
    }

    public SystemPropertyCenter(@Nonnull String[] args)
            throws URISyntaxException, ParserConfigurationException, SAXException {
        CommandLineParser parser = new BasicParser();
        Options options = new Options();
        options.addOption("h", "help", false, "Print this help message.");
        options.addOption("v", "verbose", false, "Display debug information.");
        options.addOption("a", "application", true, "Application specified to run.");
        options.addOption(null, "spark-property-file", true,
                "File path of the spark property file.");
        options.addOption(null, "system-property-file", true,
                "File path of the system property file.");
        options.addOption(null, "app-property-file", true,
                "File path of the application-specific system property file.");
        options.addOption(null, "log4j-property-file", true,
                "File path of the log4j property file.");
        options.addOption(null, "report-listening-addr", true,
                "Address of runtime report listener.");
        options.addOption(null, "report-listening-port", true,
                "Port of runtime report listener.");
        CommandLine commandLine;

        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            e.printStackTrace();
            logger.debug("Try using '-h' for more information.");
            System.exit(0);
            return;
        }

        if (commandLine.hasOption('h')) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("LaS-VPE Platform", options);
            System.exit(0);
            return;
        }

        verbose = commandLine.hasOption('v');
        if (verbose) {
            logger.setLevel(Level.DEBUG);
        }

        if (commandLine.hasOption('a')) {
            appsToStart = commandLine.getOptionValues('a');
            logger.debug("[INFO]To run application:");
            for (String app : appsToStart) {
                logger.debug("\t\t" + app);
            }
        }

        if (commandLine.hasOption("system-property-file")) {
            sysPropFilePath = commandLine.getOptionValue("system-property-file");
        }
        if (commandLine.hasOption("log4j-property-file")) {
            log4jPropFilePath = commandLine.getOptionValue("log4j-property-file");
        }
        if (commandLine.hasOption("spark-property-file")) {
            sparkConfFilePath = commandLine.getOptionValue("spark-property-file");
        }
        if (commandLine.hasOption("app-property-file")) {
            appPropFilePath = commandLine.getOptionValue("app-property-file");
        }

        // Load properties from file.
        BufferedInputStream propInputStream;
        try {
            if (sysPropFilePath.contains("hdfs:/")) {
                logger.debug("Loading system-wise default properties using HDFS platform from "
                        + sysPropFilePath + "...");
                FileSystem hdfs = FileSystem.get(new URI(sysPropFilePath), HadoopHelper.getDefaultConf());
                FSDataInputStream hdfsInputStream = hdfs.open(new Path(sysPropFilePath));
                propInputStream = new BufferedInputStream(hdfsInputStream);
            } else {
                logger.debug("Loading system-wise default properties locally from "
                        + sysPropFilePath + "...");
                propInputStream = new BufferedInputStream(new FileInputStream(sysPropFilePath));
            }
            sysProps.load(propInputStream);
            propInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Cannot find system-wise default property file at specified path: \""
                    + sysPropFilePath + "\"!\n");
            logger.error("Try use '-h' for more information.");
            System.exit(0);
            return;
        }

        if (appPropFilePath != null) {
            try {
                if (appPropFilePath.contains("hdfs:/")) {
                    logger.debug("Loading application-specific properties"
                            + " using HDFS platform from "
                            + appPropFilePath + "...");
                    FileSystem hdfs = FileSystem.get(
                            new URI(appPropFilePath),
                            HadoopHelper.getDefaultConf());
                    FSDataInputStream hdfsInputStream =
                            hdfs.open(new Path(appPropFilePath));
                    propInputStream = new BufferedInputStream(hdfsInputStream);
                } else {
                    logger.debug("Loading application-specific properties locally from "
                            + appPropFilePath + "...");
                    propInputStream = new BufferedInputStream(new FileInputStream(appPropFilePath));
                }
                sysProps.load(propInputStream);
                propInputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("Cannot find application-specific property file at specified path: \""
                        + appPropFilePath + "\"!\n");
                logger.error("Try use '-h' for more information.");
                System.exit(0);
                return;
            }
        }

        // Digest the settings.
        for (Entry<Object, Object> entry : sysProps.entrySet()) {
            if (verbose) {
                logger.debug("Read from property file: " + entry.getKey()
                        + "=" + entry.getValue());
            }
            switch ((String) entry.getKey()) {
                case "zookeeper.connect":
                    zkConn = (String) entry.getValue();
                    break;
                case "kafka.bootstrap.servers":
                    kafkaBootstrapServers = (String) entry.getValue();
                    break;
                case "kafka.partitions":
                    kafkaNumPartitions = new Integer((String) entry.getValue());
                    break;
                case "kafka.replication.factor":
                    kafkaReplFactor = new Integer((String) entry.getValue());
                    break;
                case "kafka.message.max.bytes":
                    kafkaMsgMaxBytes = new Integer((String) entry.getValue());
                    break;
                case "spark.checkpoint.dir":
                    checkpointRootDir = (String) entry.getValue();
                    break;
                case "vpe.metadata.dir":
                    metadataDir = (String) entry.getValue();
                    break;
                case "spark.master":
                    sparkMaster = (String) entry.getValue();
                    break;
                case "spark.deploy.mode":
                    sparkDeployMode = (String) entry.getValue();
                    break;
                case "vpe.platform.jar":
                    jarPath = (String) entry.getValue();
                    break;
                case "yarn.resource.manager.hostname":
                    yarnResourceManagerHostname = (String) entry.getValue();
                    break;
                case "hdfs.default.NAME":
                    hdfsDefaultName = (String) entry.getValue();
                    break;
                case "executor.num":
                    numExecutors = new Integer((String) entry.getValue());
                    break;
                case "executor.memory":
                    executorMem = (String) entry.getValue();
                    break;
                case "executor.cores":
                    executorCores = new Integer((String) entry.getValue());
                    break;
                case "total.executor.cores":
                    totalExecutorCores = new Integer((String) entry.getValue());
                    break;
                case "driver.memory":
                    driverMem = (String) entry.getValue();
                    break;
                case "driver.cores":
                    driverCores = new Integer((String) entry.getValue());
                    break;
                case "hadoop.queue":
                    hadoopQueue = (String) entry.getValue();
                    break;
                case "vpe.recv.parallel":
                    numRecvStreams = new Integer((String) entry.getValue());
                    break;
                case "vpe.buf.duration":
                    bufDuration = new Integer((String) entry.getValue());
                    break;
                case "vpe.process.time":
                    procTime = new Integer((String) entry.getValue());
                    break;
                case "kafka.max.request.size":
                    kafkaMaxRequestSize = new Integer((String) entry.getValue());
                    break;
            }
        }
    }

    /**
     * Generate command line options for SparkSubmit client, according to the
     * stored properties.
     *
     * @return An array of string with format required by SparkSubmit client.
     * @throws NoAppSpecifiedException When no application is specified to run.
     */
    private String[] getArgs() throws NoAppSpecifiedException {
        ArrayList<String> optList = new ArrayList<>();

        if (verbose) {
            optList.add("-v");
        }

        if (appPropFilePath != null && new File(appPropFilePath).exists()) {
            optList.add("--app-property-file");
            optList.add(new File(appPropFilePath).getName());
        }

        optList.add("--system-property-file");
        optList.add(new File(sysPropFilePath).getName());

        optList.add("--log4j-property-file");
        if (sparkMaster.toLowerCase().contains("yarn")) {
            optList.add("log4j.properties");
        } else if (sparkMaster.toLowerCase().contains("local")) {
            optList.add(log4jPropFilePath);
        } else {
            throw new NotImplementedException(
                    "System is currently not supporting deploy mode: "
                            + sparkMaster);
        }

        return Arrays.copyOf(optList.toArray(), optList.size(), String[].class);
    }

    public SparkLauncher GetLauncher(String appName) throws IOException {
        SparkLauncher launcher = new SparkLauncher()
                .setAppResource(jarPath)
                .setMainClass(AppManager.getMainClassName(appName))
                .setMaster(sparkMaster)
                .setAppName(appName)
                .setVerbose(verbose)
                .addFile(ConfManager.getConcatCfgFilePathList(","))
                .setConf(SparkLauncher.DRIVER_MEMORY, driverMem)
                .setConf(SparkLauncher.EXECUTOR_MEMORY, executorMem)
                .setConf(SparkLauncher.CHILD_PROCESS_LOGGER_NAME, appName)
                .setConf(SparkLauncher.EXECUTOR_CORES, "" + executorCores)
                .addSparkArg("--driver-cores", "" + driverCores)
                .addSparkArg("--num-executors", "" + numExecutors)
                .addSparkArg("--total-executor-cores", "" + totalExecutorCores)
                .addSparkArg("--queue", hadoopQueue)
                .addAppArgs(getArgs());
        if (sparkConfFilePath != null) {
            if (new File(sparkConfFilePath).exists()) {
                launcher = launcher.setPropertiesFile(sparkConfFilePath);
            } else {
                logger.warn("Spark configuration file " + sparkConfFilePath + " does not exist!");
            }
        }
        if (log4jPropFilePath != null) {
            if (new File(log4jPropFilePath).exists()) {
                launcher = launcher.addFile(log4jPropFilePath);
            } else {
                logger.warn("Loj4j configuration file " + log4jPropFilePath + " does not exist!");
            }
        }
        if (sysPropFilePath != null) {
            if (new File(sysPropFilePath).exists()) {
                launcher = launcher.addFile(sysPropFilePath);
            } else {
                logger.warn("System configuration file " + sysPropFilePath + " does not exist!");
            }
            launcher = launcher.addFile(sysPropFilePath);
        }
        if (appPropFilePath != null) {
            if (new File(appPropFilePath).exists()) {
                launcher = launcher.addFile(appPropFilePath);
            } else {
                logger.warn("App configuration file " + appPropFilePath + " does not exist!");
            }
        }
        return launcher;
    }

    /**
     * Thrown when no application is specified in any possible property sources.
     *
     * @author Ken Yu, CRIPAC, 2016
     */
    public static class NoAppSpecifiedException extends RuntimeException {
        private static final long serialVersionUID = -8356206863229009557L;
    }

    public Properties generateKafkaProducerProp(boolean isStringValue) {
        Properties producerProp = new Properties();
        producerProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        producerProp.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, kafkaMaxRequestSize);
        producerProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                isStringValue ? StringSerializer.class : ByteArraySerializer.class);
        producerProp.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "" + kafkaMsgMaxBytes);
        return producerProp;
    }

    public Properties generateKafkaConsumerProp(String group, boolean isStringValue) {
        Properties consumerProp = new Properties();
        consumerProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        consumerProp.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        consumerProp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        consumerProp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                isStringValue ? StringDeserializer.class : ByteArrayDeserializer.class);
        return consumerProp;
    }

    public Map<String, String> generateKafkaParams(String group) {
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "largest");
        kafkaParams.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "" + kafkaMsgMaxBytes);
        kafkaParams.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "" + kafkaMsgMaxBytes);
        kafkaParams.put("fetch.message.max.bytes", "" + kafkaMsgMaxBytes);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        kafkaParams.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, "" + kafkaMsgMaxBytes);
        kafkaParams.put(ConsumerConfig.SEND_BUFFER_CONFIG, "" + kafkaMsgMaxBytes);
        return kafkaParams;
    }
}
