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
package org.casia.cripac.isee.vpe.data;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.casia.cripac.isee.pedestrian.tracking.Track;

/**
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public abstract class HDFSConnector {

	protected Configuration hdfsConf;
	protected FileSystem fileSystem;

	public HDFSConnector() throws IOException {
		hdfsConf = new Configuration();
		hdfsConf.setBoolean("dfs.support.append", true);
		fileSystem = FileSystem.get(hdfsConf);
	}

	public abstract Track getTrack(String path);
}
