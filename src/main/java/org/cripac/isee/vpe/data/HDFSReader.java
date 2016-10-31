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

package org.cripac.isee.vpe.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The HDFSReader class is the base class for classes that read and parse data
 * from HDFS.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class HDFSReader {

    protected Configuration conf;
    protected FileSystem hdfs;

    /**
     * Create a HDFSReader with default configuration.
     *
     * @throws IOException On error opening Hadoop Filesystem.
     */
    public HDFSReader() throws IOException {
        conf = new Configuration();
        conf.setBoolean("dfs.support.append", true);
        hdfs = FileSystem.get(conf);
    }

    /**
     * If the path specifies a directory, list all the sub-files in it.
     * If the path specifies a file, return the path itself.
     *
     * @param path The path of a directory or a file.
     * @return Paths of sub-files if the path specifies a directory or
     * the given path itself if the path specifies a file.
     * @throws IOException On error reading files in Hadoop Filesystem.
     */
    public List<Path> listSubfiles(@Nonnull Path path) throws IOException {
        FileStatus[] fileStatus = hdfs.listStatus(path);
        Path[] listPath = FileUtil.stat2Paths(fileStatus);
        ArrayList<Path> subfilePaths = new ArrayList<>();
        ArrayList<Path> subdirPaths = new ArrayList<>();
        for (int i = 0; i < fileStatus.length; ++i) {
            if (fileStatus[i].isDirectory()) {
                subdirPaths.add(listPath[i]);
            } else {
                subfilePaths.add(listPath[i]);
            }
        }
        for (Path sdp : subdirPaths) {
            subfilePaths.addAll(listSubfiles(sdp));
        }
        return subfilePaths;
    }
}
