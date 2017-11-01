package org.cripac.isee.vpe.entities;

import java.util.List;

public class Report {

	public long usedMem;
	public long jvmMaxMem;
	public long jvmTotalMem;
	public long physicTotalMem;
	public int procCpuLoad;
	public int sysCpuLoad;
	public DevInfo[] devInfos;
	public List<Integer> devNumList;
	public List<Integer> processNumList;

    public static class DevInfo {
    	//编号
    	public int index;
    	public int fanSpeed;
        //GPU使用率
    	public int utilRate;
    	public long usedMem;
    	public long totalMem;
    	public int temp;
    	public int slowDownTemp;
    	public int shutdownTemp;
    	public int powerUsage;
    	public int powerLimit;
        //正在运行的gpu的程序个数
    	public int infoCount;
//        ProcessesDevInfo[] processesDevInfos;
//        private static class ProcessesDevInfo {
//        	//正在使用的gpu id
//        	int pid;
//        	//正在使用的gpu 内存
//        	long usedGpuMemory;
//        }
    }
    

}
