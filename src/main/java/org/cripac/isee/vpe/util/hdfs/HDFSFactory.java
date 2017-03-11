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

package org.cripac.isee.vpe.util.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.cripac.isee.vpe.util.Factory;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * The HDFSFactory is used to get or create Hadoop DistributedFileSystem instances.
 */
public class HDFSFactory implements Factory<FileSystem> {
    private static final long serialVersionUID = 2721843832976362696L;

    /**
     * Produce a new Hadoop DistributedFileSystem instance.
     *
     * @return a new DistributedFileSystem newly produced.
     * @throws IOException on failure creating a new instance.
     */
    @Nonnull
    @Override
    public FileSystem produce() throws IOException {
        return newInstance();
    }

    /**
     * Produce a new Hadoop DistributedFileSystem instance.
     *
     * @return a new DistributedFileSystem newly produced.
     * @throws IOException on failure creating a new instance.
     */
    public static FileSystem newInstance() throws IOException {
        Configuration hdfsConf = HadoopHelper.getDefaultConf();
        hdfsConf.setBoolean("fs.hdfs.impl.disable.cache", true);
        return FileSystem.newInstance(hdfsConf);
    }

    /**
     * Get an existing Hadoop DistributedFileSystem instance. If no, create a new one.
     *
     * @return a DistributedFileSystem instance.
     * @throws IOException on failure creating a new instance.
     */
    public static FileSystem get() throws IOException {
        Configuration hdfsConf = HadoopHelper.getDefaultConf();
        return FileSystem.get(hdfsConf);
    }
}
