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
package org.casia.cripac.isee.vpe.ctrl;

import java.util.HashMap;
import java.util.Map;

import org.casia.cripac.isee.vpe.alg.PedestrianAttrRecogApp;
import org.casia.cripac.isee.vpe.alg.PedestrianReIDWithAttrApp;
import org.casia.cripac.isee.vpe.alg.PedestrianTrackingApp;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter.NoAppSpecifiedException;
import org.casia.cripac.isee.vpe.data.DataManagingApp;

/**
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class AppManager {

	private static Map<String, String> classNameMap = new HashMap<>();

	static {
		classNameMap.put(PedestrianReIDWithAttrApp.APP_NAME, PedestrianReIDWithAttrApp.class.getName());
		classNameMap.put(PedestrianTrackingApp.APP_NAME, PedestrianTrackingApp.class.getName());
		classNameMap.put(PedestrianAttrRecogApp.APP_NAME, PedestrianAttrRecogApp.class.getName());
		classNameMap.put(DataManagingApp.APP_NAME, DataManagingApp.class.getName());
		classNameMap.put(MessageHandlingApp.APP_NAME, MessageHandlingApp.class.getName());
	}

	public static String getMainClassName(String appName) throws NoAppSpecifiedException {
		System.out.println("|INFO|Searching class name of App " + appName + "...");
		if (classNameMap.containsKey(appName))
			return classNameMap.get(appName);
		else
			throw new NoAppSpecifiedException();
	}
}
