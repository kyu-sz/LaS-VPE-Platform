/***********************************************************************
 * This file is part of VPE-Platform.
 * 
 * VPE-Platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * VPE-Platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with VPE-Platform.  If not, see <http://www.gnu.org/licenses/>.
 ************************************************************************/

package org.casia.cripac.isee.vpe.common;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Properties;

public class SystemPropertyCenter {
	
	//Zookeeper properties
	public String zookeeper = "localhost:2181";
	public int sessionTimeoutMs = 10 * 10000;
	public int connectionTimeoutMs = 8 * 1000;
	
	//Kafka properties
	public String kafkaBrokers = "localhost:9092";
	public int partitions = 1;
	public int replicationFactor = 1;
	
	//Spark properties
	public String checkpointDir = "checkpoint";
	public String sparkMaster = "local[*]";
	
	public SystemPropertyCenter() { }
	
	public SystemPropertyCenter(String propertyFilename) throws IOException {
		//Load the property file.
		Properties systemProperties = new Properties();
		BufferedInputStream propInputStream = new BufferedInputStream(new FileInputStream(propertyFilename));
		systemProperties.load(propInputStream);
		
		//Digest the settings.
		for (Entry<Object, Object> entry : systemProperties.entrySet()) {
			System.out.println("Read property: " + entry.getKey() + " - " + entry.getValue());
			
			switch ((String) entry.getKey()) {
			case "zookeeper":
				zookeeper = (String) entry.getValue(); 
				break;
			case "kafka.brokers":
				kafkaBrokers = (String) entry.getValue(); 
				break;
			case "partitions":
				partitions = new Integer((String) entry.getValue()); 
				break;
			case "replication.factor":
				replicationFactor = new Integer((String) entry.getValue()); 
				break;
			case "checkpoint.directory":
				checkpointDir = (String) entry.getValue(); 
				break;
			case "spark.master":
				sparkMaster = (String) entry.getValue(); 
				break;
			}
		}
	}
}
