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

package org.cripac.isee.vpe.ctrl;

import org.apache.commons.io.IOUtils;
import org.cripac.isee.vpe.util.tracking.DirectoryHierarchy;
import org.cripac.isee.vpe.util.tracking.FileDescriptor;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The class ConfigFileManager manages configuration files other than several
 * preset ones to be uploaded to Spark. Note that it is only usable before
 * the system is submitted to YARN.
 * <p>
 * <p/>
 * Created by ken.yu on 16-10-8.
 */
public class ConfigFileManager {
    /**
     * Get a list of all the configuration files registered under the path.
     *
     * @param path A length-variable array of names of hierarchies.
     * @return A list of descriptors of all the configuration files.
     */
    public static List<FileDescriptor> getConfigFileList(@Nonnull String... path) {
        DirectoryHierarchy dir = top;
        for (String name : path) {
            dir = dir.getSubHierarchy(name);
        }
        return dir.gatherFiles();
    }

    /**
     * Get a concatenated string list of paths of temporary configuration files.
     * The temporary configuration files are copied from those in this project,
     * and stored in the temporary directory.
     * On the first call of this method, the ConfigManager will automatically
     * prepare the temporary files. The files are automatically deleted on the
     * exit of JVM.
     *
     * @return A string of concatenated paths.
     * @throws IOException On error preparing temporary configuration files.
     */
    public static String getConcatConfigFilePathList(@Nonnull String connector,
                                                     @Nonnull String... path) throws IOException {
        if (tmpDir == null) {
            prepareTmpConfigFiles();
        }
        return getConfigFileList(path).stream()
                .map(descriptor -> tmpDir + "/" + descriptor.getConcatName())
                .collect(Collectors.joining(connector));
    }

    private static void prepareTmpConfigFiles() throws IOException {
        tmpDir = System.getProperty("java.io.tmpdir");
        List<FileDescriptor> fileList = ConfigFileManager.getConfigFileList("pedestrian-tracking");
        for (FileDescriptor file : fileList) {
            File tmp = new File(tmpDir + "/" + file.getConcatName());
            tmp.createNewFile();
            FileOutputStream tmpOutputStream = new FileOutputStream(tmp);
            IOUtils.copy(new FileInputStream(CONF_DIR + "/" + file.getPath()), tmpOutputStream);
            tmpOutputStream.close();
            tmp.deleteOnExit();
        }
    }

    private final static String CONF_DIR = "conf";

    private static String tmpDir = null;

    /**
     * Top group corresponding to the folder where configurations are stored.
     */
    private static DirectoryHierarchy top;

    /**
     * Register all the new configuration files here.
     */
    static {
        top = DirectoryHierarchy.createTop(CONF_DIR);

        // Register pedestrian tracking configuration file hierarchy.
        DirectoryHierarchy pedestrianTracking =
                new DirectoryHierarchy("pedestrian-tracking", top);

        // Register ISEE Basic Pedestrian Tracker configuration files.
        DirectoryHierarchy iseeBasicPedestrianTracking =
                new DirectoryHierarchy("isee-basic", pedestrianTracking);
        iseeBasicPedestrianTracking.addFiles(".*.conf");
    }

    /**
     * The ConfigFileManager cannot be instantiated.
     */
    private ConfigFileManager() {
    }
}
