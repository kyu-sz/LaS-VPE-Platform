/***********************************************************************
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
 * along with LaS-VPE-Platform.  If not, see <http://www.gnu.org/licenses/>.
 ************************************************************************/
package org.casia.cripac.isee.vpe.debug;

import java.io.IOException;

import org.casia.cripac.isee.pedestrian.tracking.Track;
import org.casia.cripac.isee.vpe.data.HDFSConnector;

/**
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class FakeHDFSConnector extends HDFSConnector {

	/**
	 * @throws IOException
	 */
	public FakeHDFSConnector() throws IOException {
		super();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.casia.cripac.isee.vpe.data.HDFSConnector#getTrack(java.lang.String)
	 */
	@Override

	public Track getTrack(String path) {
		Track[] tracks = new FakePedestrianTracker().track("video123");
		return tracks[0];
	}

}
