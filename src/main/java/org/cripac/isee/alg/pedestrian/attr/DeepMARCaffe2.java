/*
 * This file is part of LaS-VPE-Platform.
 *
 * LaS-VPE-Platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * LaS-VPE-Platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with LaS-VPE-Platform. If not, see <http://www.gnu.org/licenses/>.
 *
 * Created by ken.yu on 17-3-27.
 */
package org.cripac.isee.alg.pedestrian.attr;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;

import static org.cripac.isee.util.ResourceManager.getResource;

/**
 * The interface DeepMARCaffe2 defines methods of retrieving the network protobuf and model for DeepMAR implementations
 * that are based on Caffe2.
 */
public interface DeepMARCaffe2 extends DeepMAR {

    @Nonnull
    static File getInitNetProtobuf() throws IOException {
        return getResource("/models/DeepMARCaffe2/init_net.pb");
    }

    @Nonnull
    static File getPredictNetModel() throws IOException {
        return getResource("/models/DeepMARCaffe2/predict_net.pb");
    }
}
