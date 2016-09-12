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

package org.casia.cripac.isee.pedestrian.tracking;

import java.io.Serializable;

import com.google.gson.annotations.SerializedName;

/**
 * The Track class stores a sequence of bounding boxes, representing the track
 * of a pedestrian in a video.
 * 
 * TODO Should assign an ID to each track or use some other methods to identify
 * them.
 * 
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class Track implements Serializable {

	/**
	 * Identifier of a track.
	 * 
	 * @author Ken Yu, CRIPAC, 2016
	 *
	 */
	public static class Identifier implements Serializable {

		private static final long serialVersionUID = -4575631684877590629L;

		/**
		 * The URL indicating where the source video is stored.
		 */
		@SerializedName("video-url")
		public char[] videoURL = null;

		/**
		 * The serial number of the track in the video. The number is usually
		 * organized in a chronological order. Track generators, like pedestrian
		 * tracking modules, should be responsible of generating this ID. Its
		 * default value is -1, meaning the ID has not been generated yet.
		 */
		@SerializedName("serial-number")
		public int serialNumber = -1;

		/**
		 * Create an identifier with URL of source video and the serial number.
		 * 
		 * @param videoURL
		 * @param num
		 */
		public Identifier(String videoURL, int num) {
			this.videoURL = videoURL.toCharArray();
			this.serialNumber = num;
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
			return new String(videoURL) + "_tarid" + serialNumber;
		}
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
		 * x-coordinate of the point on the left-upper corner of the bounding
		 * box.
		 */
		public int x = 0;

		/**
		 * y-coordinate of the point on the left-upper corner of the bounding
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
		 * The RGB data of the patch croped by the bounding box. If it is not
		 * availble, it should be set to null. Otherwise, it should follow the
		 * format of the 'data' field of OpenCV 2.x's Mat class. When
		 * reconstructing a OpenCV's Mat with this bounding box, first allocate
		 * a Mat with width and height same to this bounding box and format as
		 * CV_8UC3, then directly copy data of the 'patchData' field here to the
		 * 'data' field of it using functions like memcpy.
		 */
		public byte[] patchData = null;

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return "@Bounding box " + "x: " + x + ", " + "y: " + y + ", " + "width: " + width + ", " + "height: "
					+ height;
		}
	}

	private static final long serialVersionUID = -6133927313545472821L;

	/**
	 * The identifier of the track.
	 */
	public Identifier id = new Identifier();

	/**
	 * The total number of tracks in the video. This field can be left as -1,
	 * meaning that this information is not available yet, but setting this
	 * field can help checking whether all tracks have been obtained or
	 * processed in latter steps. So it is highly recommended that track
	 * generators should also fill in this field.
	 */
	@SerializedName("track-number")
	public int numTracks = -1;

	/**
	 * The starting frame index of this track in the source video. User can use
	 * this to calculate the starting time of this track using the FPS
	 * information of the video. Its default value is -1, meaning the index has
	 * not been determined yet.
	 */
	@SerializedName("start-frame-index")
	public int startFrameIndex = -1;

	/**
	 * Storing the locations with the BoundingBox class at each moment, sorted
	 * by time.
	 */
	@SerializedName("bounding-boxes")
	public BoundingBox[] locationSequence = null;

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append("@Track\n");
		sb.append("Video URL: " + new String(id.videoURL) + "\n");
		sb.append("Serial number: " + id.serialNumber + "/" + numTracks + "\n");
		sb.append("Start frame: " + startFrameIndex + "\n");
		for (int i = 0; i < locationSequence.length; ++i) {
			sb.append("At frame " + (startFrameIndex + i) + ": " + locationSequence[i] + "\n");
		}

		return sb.toString();
	}
}