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

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.xml.sax.SAXException;

/**
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class HadoopUtils {
	public static Configuration getDefaultConf() throws ParserConfigurationException, SAXException, IOException {
		//Load Hadoop configuration from XML files.
		Configuration hadoopConf = new Configuration();
		Map<String, String> propMap;
		propMap = HadoopXMLParser.getPropsFromXML(new File("$HADOOP/etc/hadoop/core.xml"));
		for (Entry<String, String> prop : propMap.entrySet()) {
			System.out.printf("Setting hadoop configuration: %s=%s\n", prop.getKey(), prop.getValue());
			hadoopConf.set(prop.getKey(), prop.getValue());
		}
		propMap = HadoopXMLParser.getPropsFromXML(new File("$HADOOP/etc/hadoop/yarn.xml"));
		for (Entry<String, String> prop : propMap.entrySet()) {
			System.out.printf("Setting hadoop configuration: %s=%s\n", prop.getKey(), prop.getValue());
			hadoopConf.set(prop.getKey(), prop.getValue());
		}
		return hadoopConf;
	}
}
