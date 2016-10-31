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

package org.cripac.isee.vpe.debug;

import org.cripac.isee.vpe.util.hdfs.HDFSVideoDecoder;
import org.cripac.isee.vpe.util.tracking.VideoData;

import javax.annotation.Nonnull;
import java.util.Random;

/**
 * @author Ken Yu, CRIPAC, 2016
 */
public class FakeHDFSVideoDecoder extends HDFSVideoDecoder {

    private static Random rand = new Random();

    /*
     * (non-Javadoc)
     *
     * @see org.casia.cripac.isee.vpe.common.HDFSVideoDecoder#decode(java.lang.
     * String)
     */
    @Override
    public VideoData decode(@Nonnull String videoURL) {
        VideoData data = new VideoData();
        data.width = 1280;
        data.height = 720;
        data.channels = 3;
        int numFrames = rand.nextInt(100);
        data.frames = new byte[numFrames][];
        for (int i = 0; i < numFrames; ++i) {
            byte[] frameData = new byte[data.width * data.height * data.channels];
            data.frames[i] = frameData;
        }
        return data;
    }

}
