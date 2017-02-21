/*
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
 */

package org.cripac.isee.vpe.ctrl;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.cripac.isee.vpe.alg.PedestrianAttrRecogApp;
import org.cripac.isee.vpe.alg.PedestrianReIDUsingAttrApp;
import org.cripac.isee.vpe.alg.PedestrianTrackingApp;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter.NoAppSpecifiedException;
import org.cripac.isee.vpe.data.DataManagingApp;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * @author Ken Yu, CRIPAC, 2016
 */
public class AppManager {

    private static Map<String, String> classNameMap = new Object2ObjectOpenHashMap<>();

    static {
        classNameMap.put(PedestrianReIDUsingAttrApp.APP_NAME, PedestrianReIDUsingAttrApp.class.getName());
        classNameMap.put(PedestrianTrackingApp.APP_NAME, PedestrianTrackingApp.class.getName());
        classNameMap.put(PedestrianAttrRecogApp.APP_NAME, PedestrianAttrRecogApp.class.getName());
        classNameMap.put(DataManagingApp.APP_NAME, DataManagingApp.class.getName());
        classNameMap.put(MessageHandlingApp.APP_NAME, MessageHandlingApp.class.getName());
    }

    /**
     * Private constructor - this class will never be instanced
     */
    private AppManager() {
    }

    public static String getMainClassName(@Nonnull String appName) throws NoAppSpecifiedException {
        if (classNameMap.containsKey(appName))
            return classNameMap.get(appName);
        else
            throw new NoAppSpecifiedException("Cannot find application " + appName);
    }
}
