/*
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
 * along with LaS-VPE-Platform. If not, see <http://www.gnu.org/licenses/>.
 *
 * Created by ken.yu on 17-3-31.
 */
package org.cripac.isee.util;

import org.apache.commons.io.IOUtils;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;

import java.io.*;

public class ResourceManager {
    /**
     * Find a resource in the JAR file.
     * If failed (e.g. it is not in the JAR file or the program is not run from a JAR),
     * try to locate the resource at java.library.path.
     *
     * @param path path to the resource starting with a slash (e.g. /lib/x64/libcaffe.so).
     * @return a file with the same content as the required resource.
     * @throws IOException on failure locating or opening the resource.
     */
    public static File getResource(String path) throws IOException {
        // Retrieve the file from JAR and store to temporary files.
        InputStream in = ResourceManager.class.getResourceAsStream(path);
        if (in == null) {
//            File externalResource = new File(System.getProperty("java.library.path") + path);
            File externalResource = new File(SystemPropertyCenter.projectLocation+ path);
            if (!externalResource.exists()) {
                throw new FileNotFoundException("Cannot locate resource at " + path);
            } else {
                return externalResource;
            }
        }

        int lastSlash = path.replace('\\', '/').lastIndexOf('/');
        String filename = path.substring(lastSlash + 1);

        File defaultTmpDir = new File("/tmp");
        try {
            File tempFile = File.createTempFile(filename, null,
                    defaultTmpDir.canWrite() ? defaultTmpDir : null);
            tempFile.deleteOnExit();
            try (OutputStream out = new FileOutputStream(tempFile)) {
                IOUtils.copy(in, out);
                return tempFile;
            }
        } finally {
            in.close();
        }
    }
}
