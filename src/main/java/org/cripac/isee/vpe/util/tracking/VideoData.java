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

package org.cripac.isee.vpe.util.tracking;

/**
 * The VideoData class wraps decoded data of a video. It is assumed that the
 * width, height and channels of each frame in a video are fixed.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class VideoData {
    /**
     * The width of each frame.
     */
    public int width;

    /**
     * The height of each frame.
     */
    public int height;

    /**
     * The channels of each frame.
     */
    public int channels;

    /**
     * All the decoded frames. The data of each frame is stored in a byte array.
     */
    public byte[][] frames = null;
}
