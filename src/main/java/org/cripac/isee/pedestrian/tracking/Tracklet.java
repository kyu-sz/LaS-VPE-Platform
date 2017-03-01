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

package org.cripac.isee.pedestrian.tracking;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The Tracklet class stores a sequence of bounding boxes, representing the
 * track of a pedestrian in a video.
 * <p>
 * TODO Should assign an IDRANK to each track or use some other methods to identify them.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class Tracklet implements Serializable, Cloneable {

    private static final long serialVersionUID = -6133927313545472821L;
    /**
     * The identifier of the track.
     */
    public Identifier id = new Identifier();
    /**
     * The total number of tracklets in the video. This field can be left as -1,
     * meaning that this information is not available yet, but setting this
     * field can help checking whether all tracklets have been obtained or
     * processed in latter steps. So it is highly recommended that track
     * generators should also fill in this field.
     */
    @SerializedName("tracklet-number")
    public int numTracklets = -1;
    /**
     * The starting frame index of this track in the source video. User can use
     * this to calculate the starting time of this track using the FPS
     * information of the video. Its default value is -1, meaning the index has
     * not been determined yet.
     */
    @SerializedName("run-frame-index")
    public int startFrameIndex = -1;
    /**
     * A sequence of locations (as a form of BoundingBox) of the pedestrian
     * at each moment in the tracklet.
     */
    @SerializedName("bounding-boxes")
    public BoundingBox[] locationSequence = null;

    /**
     * Transform this tracklet to Json string.
     *
     * @return a json string of this tracklet.
     */
    @Override
    public String toString() {
        return new Gson().toJson(this);
    }

    /**
     * Identifier of a tracklet.
     *
     * @author Ken Yu, CRIPAC, 2016
     */
    public static class Identifier implements Serializable {

        private static final long serialVersionUID = -4575631684877590629L;

        /**
         * Identifier of the video where the track is generated.
         * This can be the storing URL of the video,
         * or a combination of the WEBCAM_LOGIN_PARAM address real-time camera and
         * the starting time of the video fragment.
         */
        @SerializedName("video-url")
        public String videoID = "unknown";

        /**
         * The serial number of the track in the video. The number is usually
         * organized in a chronological order. Tracklet generators, like pedestrian
         * tracking modules, should be responsible of generating this IDRANK. Its
         * default value is -1, meaning the IDRANK has not been generated yet.
         */
        @SerializedName("serial-number")
        public int serialNumber = -1;

        /**
         * Create an identifier with URL of source video and the serial number.
         *
         * @param videoID      Identifier of the video where the track is generated.
         *                     This can be the storing URL of the video,
         *                     or a combination of the WEBCAM_LOGIN_PARAM address real-time camera and
         *                     the starting time of the video fragment.
         * @param serialNumber The serial number of the track in the video (1, 2, 3...).
         */
        public Identifier(@Nonnull String videoID, int serialNumber) {
            this.videoID = videoID;
            this.serialNumber = serialNumber;
        }

        /**
         * Create an empty identifier.
         */
        public Identifier() {
        }

        /*
         * (non-Javadoc)
         *
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return videoID + "_tarid" + serialNumber;
        }
    }

    /**
     * Select some bounding boxes as the appearance samples of the pedestrian.
     * Only these bounding boxes retain the patch data, and patch data in other
     * bounding boxes will be cleared, so as the tracklet consumes less memory.
     *
     * @param numSamples number of samples to be selected.
     */
    public void sample(int numSamples) {
        switch (numSamples) {
            case -1:
                // Do not perform sampling.
                break;
            case 0:
                // Clear all patch data.
                for (BoundingBox box : locationSequence) {
                    box.patchData = null;
                }
                break;
            default:
                // Perform even sampling.
                final int interval = locationSequence.length / (numSamples + 1);
                final int start = (locationSequence.length - (numSamples - 1) * interval) / 2;
                for (int i = 0; i < locationSequence.length; ++i) {
                    if (((i - start) % interval) != 0) {
                        locationSequence[i].patchData = null;
                    }
                }
                break;
        }
    }

    public Collection<BoundingBox> getSamples() {
        List<BoundingBox> samples = new ArrayList<>();
        for (BoundingBox box : locationSequence) {
            if (box.patchData != null) {
                samples.add(box);
            }
        }
        return samples;
    }

    /**
     * The BoundingBox class stores the location of an object in a single static
     * frame.
     *
     * @author Ken Yu, CRIPAC, 2016
     */
    public static class BoundingBox implements Serializable {

        private static final long serialVersionUID = -5261437055893590056L;

        /**
         * x-coordinator of the point on the left-upper corner of the bounding
         * box.
         */
        public int x = 0;

        /**
         * y-coordinator of the point on the left-upper corner of the bounding
         * box.
         */
        public int y = 0;

        /**
         * The width of the bounding box.
         */
        public int width = 0;

        /**
         * The height of the bounding box.
         */
        public int height = 0;

        /**
         * The RGB data of the patch croped by the bounding box. If not
         * available, it should be set to null. Otherwise, it should follow the
         * format of the 'data' field of OpenCV 2.x's Mat class. When
         * reconstructing a OpenCV's Mat with this bounding box, first allocate
         * a Mat with width and height same to this bounding box and format as
         * CV_8UC3, then directly copy data of the 'patchData' field here to the
         * 'data' field of it using functions like memcpy.
         */
        public byte[] patchData = null;

        /**
         * Transform the bounding box to a Json string.
         * The string contains location information only (no pixel data).
         *
         * @return A Json string representing the location information of the bounding box.
         */
        @Override
        public String toString() {
            return "{x=" + x + ", y=" + y + ", width=" + width + ", height=" + height + "}";
        }

        /**
         * Transform the bounding box into a byte array.
         *
         * @return 16 bytes representing x, y, width and height,
         * then width * height * 3 bytes representing the pixels in the patch.
         */
        public byte[] toBytes() {
            ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES * 4
                    + (patchData == null ? 0 : patchData.length));
            buf.putInt(x);
            buf.putInt(y);
            buf.putInt(width);
            buf.putInt(height);
            if (patchData != null) {
                buf.put(patchData);
            }
            return buf.array();
        }
    }
}