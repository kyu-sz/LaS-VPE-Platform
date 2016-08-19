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

import org.casia.cripac.isee.vpe.common.HDFSVideoDecoder;
import org.casia.cripac.isee.vpe.common.VideoData;
import org.casia.cripac.isee.vpe.debug.FakeHDFSVideoDecoder;

/**
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class NativeTracker2000 extends PedestrianTracker {

	private HDFSVideoDecoder videoDecoder = null;
	private byte[] conf = null;
	private native Track[] nativeTrack(int width, int height, int channels, byte[] conf, byte[][] frames);
	
	/**
	 * 
	 */
	public NativeTracker2000(byte[] conf) {
		this.videoDecoder = new FakeHDFSVideoDecoder();
		this.conf = conf;
	}

	/* (non-Javadoc)
	 * @see org.casia.cripac.isee.pedestrian.tracking.PedestrianTracker#track(java.lang.String)
	 */
	@Override
	public Track[] track(String videoURL) {
		VideoData videoData = videoDecoder.decode(videoURL);
		return nativeTrack(videoData.width, videoData.height, videoData.channels, conf,
				(byte[][]) videoData.frames.toArray());
	}

}
