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

package org.cripac.isee.vpe.util.hdfs;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The HadoopXMLParser class helps parsing Hadoop configuration files in XML.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class HadoopXMLParser {

    public static Map<String, String> getPropsFromXML(File xmlFile)
            throws ParserConfigurationException, SAXException, IOException {

        Map<String, String> propMap = new HashMap<>();

        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
        Document doc = documentBuilder.parse(xmlFile);

        Element root = doc.getDocumentElement();
        if (null != root && root.getNodeName().equals("configuration")) {
            NodeList nodeList = root.getChildNodes();
            for (int i = 0; i < nodeList.getLength(); ++i) {
                String key = null;
                String value = null;

                Node props = nodeList.item(i);
                NodeList keyList = props.getChildNodes();
                for (int j = 0; j < keyList.getLength(); ++j) {
                    Node item = keyList.item(j);
                    if (item.getNodeName().equals("name")) {
                        key = item.getFirstChild().getNodeValue();
                    } else if (item.getNodeName().equals("value")) {
                        value = item.getFirstChild().getNodeValue();
                    }
                }

                if (key != null && value != null) {
                    propMap.put(key, value);
                }
            }
        }

        return propMap;
    }
}
