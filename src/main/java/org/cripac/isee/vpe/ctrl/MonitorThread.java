/*
 * This file is part of las-vpe-platform.
 *
 * las-vpe-platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * las-vpe-platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with las-vpe-platform. If not, see <http://www.gnu.org/licenses/>.
 *
 * Created by ken.yu on 17-3-13.
 */
package org.cripac.isee.vpe.ctrl;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;

import javax.management.InstanceNotFoundException;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.cripac.isee.util.WebToolUtils;
import org.cripac.isee.vpe.entities.Report;
import org.cripac.isee.vpe.util.kafka.KafkaHelper;
import org.cripac.isee.vpe.util.logging.Logger;

import com.google.gson.Gson;
import com.sun.management.OperatingSystemMXBean;

public class MonitorThread extends Thread {

    public static final String REPORT_TOPIC = "monitor-desc-";

//    private static class Report {
//        long usedMem;
//        long jvmMaxMem;
//        long jvmTotalMem;
//        long physicTotalMem;
//        int procCpuLoad;
//        int sysCpuLoad;
//        DevInfo[] devInfos;
//
//        private static class DevInfo {
//        	//编号
//        	int index;
//            int fanSpeed;
//            //GPU使用率
//            int utilRate;
//            long usedMem;
//            long totalMem;
//            int temp;
//            int slowDownTemp;
//            int shutdownTemp;
//            int powerUsage;
//            int powerLimit;
//            //正在运行的gpu的程序个数
//            int infoCount;
////            ProcessesDevInfo[] processesDevInfos;
////            private static class ProcessesDevInfo {
////            	//正在使用的gpu id
////            	int pid;
////            	//正在使用的gpu 内存
////            	long usedGpuMemory;
////            }
//        }
//        
//    }

    private final Logger logger;
    private final KafkaProducer<String, String> reportProducer;
    private final Runtime runtime = Runtime.getRuntime();
    private final OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
    //gpu个数
    private final int deviceCount;
    //主机名
    private final String nodeName;
    private final String ip;
//    private final int infoCount;
    private native int getInfoCount(int index);
    
//    private native int getPid(int index);
    
//    private native long getUsedGpuMemory(int index);
    
    private native int initNVML();

    //gpu个数
    private native int getDeviceCount();
    
    private native int getFanSpeed(int index);

    //GPU使用率
    private native int getUtilizationRate(int index);

    private native long getFreeMemory(int index);

    private native long getTotalMemory(int index);

    private native long getUsedMemory(int index);

    private native int getTemperature(int index);

    private native int getSlowDownTemperatureThreshold(int index);

    private native int getShutdownTemperatureThreshold(int index);

    private native int getPowerLimit(int index);

    private native int getPowerUsage(int index);

    public MonitorThread(Logger logger, SystemPropertyCenter propCenter)
            throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException {
        this.logger = logger;
        this.reportProducer = new KafkaProducer<>(propCenter.getKafkaProducerProp(true));

        String nodeName1;
        try {
            nodeName1 = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            nodeName1 = "Unknown host";
        }
        nodeName = nodeName1;

        String ipString;
        try {
        	ipString=WebToolUtils.getLocalIP();
		} catch (Exception e) {
			// TODO: handle exception
			ipString="Unknown ip";
		}
        ip=ipString;
        
        logger.info("hostname："+this.nodeName+",ip:"+this.ip);
        
        KafkaHelper.createTopic(propCenter.zkConn, propCenter.zkSessionTimeoutMs, propCenter.zkConnectionTimeoutMS,
                REPORT_TOPIC+ip,
                propCenter.kafkaNumPartitions, propCenter.kafkaReplFactor);

        logger.info("Running with " + osBean.getAvailableProcessors() + " " + osBean.getArch() + " processors");

        int nvmlInitRet = initNVML();
        if (nvmlInitRet == 0) {
            this.deviceCount = getDeviceCount();
            logger.info("Running with " + this.deviceCount + " GPUs.");
        } else {
            this.deviceCount = 0;
            logger.info("Cannot initialize NVML: " + nvmlInitRet);
        }
        
//        this.infoCount=getInfoCount();
    }

    @Override
    public void run() {
        Report report = new Report();
        report.devInfos = new Report.DevInfo[deviceCount];
        report.devNumList=new ArrayList<>();
        report.processNumList=new ArrayList<>();
        for (int i = 0; i < deviceCount; ++i) {
            report.devInfos[i] = new Report.DevInfo();
            report.devNumList.add(i);
        }

        logger.debug("Starting monitoring gpu!");
        //noinspection InfiniteLoopStatement
//        while (true) {
            report.usedMem = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
            report.jvmMaxMem = runtime.maxMemory() / (1024 * 1024);
            report.jvmTotalMem = runtime.totalMemory() / (1024 * 1024);
            report.physicTotalMem = osBean.getTotalPhysicalMemorySize() / (1024 * 1024);
            logger.info("Memory consumption: "
                    + report.usedMem + "/"
                    + report.jvmMaxMem + "/"
                    + report.jvmTotalMem + "/"
                    + report.physicTotalMem + "M");

            report.procCpuLoad = (int) (osBean.getProcessCpuLoad() * 100);
            report.sysCpuLoad = (int) (osBean.getSystemCpuLoad() * 100);
            logger.info("CPU load: " + report.procCpuLoad + "/" + report.sysCpuLoad + "%");

            StringBuilder stringBuilder = new StringBuilder("GPU Usage:");
            for (int i = 0; i < deviceCount; ++i) {
                Report.DevInfo info = report.devInfos[i];
                info.index=i;
                info.fanSpeed = getFanSpeed(i);
                info.utilRate = getUtilizationRate(i);
                info.usedMem = getUsedMemory(i);
                info.totalMem = getTotalMemory(i);
                info.temp = getTemperature(i);
                info.slowDownTemp = getSlowDownTemperatureThreshold(i);
                info.shutdownTemp = getShutdownTemperatureThreshold(i);
                info.powerUsage = getPowerUsage(i);
                info.powerLimit = getPowerLimit(i);
                info.infoCount=getInfoCount(i);
//                info.processNumList=new ArrayList<>();
                for (int j = 0; j < info.infoCount; j++) {
//                	Report.DevInfo.ProcessesDevInfo processesDevInfo=info.processesDevInfos[j];
//                	processesDevInfo.usedGpuMemory=getUsedGpuMemory(j);
                	report.processNumList.add(i);
				}
                stringBuilder.append("\n|Index\t|Fan\t|Util\t|Mem(MB)\t|Temp(C)\t|Pow\t|infoCount");
                stringBuilder.append("\n|").append(info.index)
                        .append("\t|")
                        .append(info.fanSpeed)
                        .append("\t|")
                        .append(String.format("%3d", info.utilRate)).append('%')
                        .append("\t|")
                        .append(String.format("%5d", info.usedMem / (1024 * 1024)))
                        .append("/").append(String.format("%5d", info.totalMem / (1024 * 1024)))
                        .append("\t|")
                        .append(info.temp).append("/").append(info.slowDownTemp).append("/").append(info.shutdownTemp)
                        .append("\t|")
                        .append(info.powerUsage).append("/").append(info.powerLimit)
                		.append("\t|")
                		.append(info.infoCount);
            }
            logger.info(stringBuilder.toString());

            this.reportProducer.send(new ProducerRecord<>(REPORT_TOPIC+ip, nodeName, new Gson().toJson(report)));

//            try {
//                sleep(10000);
//            } catch (InterruptedException ignored) {
//            }
        }
//    }

    static {
        org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(MonitorThread.class);
        try {
            logger.info("Loading native libraries for MonitorThread from "
                    + System.getProperty("java.library.path"));
            System.loadLibrary("CudaMonitor4j");
//            System.load("/home/jun.li/monitor.test/src/native/CudaMonitor4j/Release/libCudaMonitor4j.so");
            logger.info("Native libraries for BasicTracker successfully loaded!");
        } catch (Throwable t) {
            logger.error("Failed to load native library for MonitorThread", t);
            throw t;
        }
    }
}
