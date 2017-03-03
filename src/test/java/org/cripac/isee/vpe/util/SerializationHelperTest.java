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
 * Created by ken.yu on 17-3-3.
 */

package org.cripac.isee.vpe.util;

import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.alg.PedestrianAttrRecogApp;
import org.cripac.isee.vpe.common.DataType;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.debug.FakePedestrianTracker;
import org.cripac.isee.vpe.util.tracking.TrackletOrURL;
import org.junit.Test;

import java.io.FileInputStream;
import java.util.Collections;

public class SerializationHelperTest {
    @Test
    public void serialize() throws Exception {
        TaskData.ExecutionPlan executionPlan = new TaskData.ExecutionPlan();
        TaskData.ExecutionPlan.Node attrNode = executionPlan.addNode(DataType.ATTRIBUTES);
        Tracklet[] tracklets = new FakePedestrianTracker().track(new FileInputStream("pom.xml"));
        TaskData taskData = new TaskData(
                Collections.singletonList(attrNode.createInputPort(PedestrianAttrRecogApp.RecogStream.TRACKLET_PORT)),
                executionPlan,
                new TrackletOrURL(tracklets[0]));
        byte[] serialized = SerializationHelper.serialize(taskData);
        int size = serialized.length;
        for (int i = 0; i < 100; ++i) {
            serialized = SerializationHelper.serialize(taskData);
            assert serialized.length == size;
        }
    }
}