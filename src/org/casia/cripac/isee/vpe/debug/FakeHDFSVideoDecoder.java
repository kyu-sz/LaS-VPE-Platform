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
package org.casia.cripac.isee.vpe.debug;

import java.util.Random;

import org.casia.cripac.isee.vpe.common.HDFSVideoDecoder;
import org.casia.cripac.isee.vpe.common.VideoData;

/**
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class FakeHDFSVideoDecoder extends HDFSVideoDecoder {

	private static Random rand = new Random(); 
	
	/* (non-Javadoc)
	 * @see org.casia.cripac.isee.vpe.common.HDFSVideoDecoder#decode(java.lang.String)
	 */
	@Override
	public VideoData decode(String videoURL) {
		VideoData data = new VideoData();
		data.width = 1280;
		data.height = 720;
		data.channels = 3;
		int numFrames = rand.nextInt(100);
		for (int i = 0; i < numFrames; ++i) {
			byte[] frameData = new byte[data.width * data.height * data.channels];
			data.frames.add(frameData);
		}
		return data;
	}

}
