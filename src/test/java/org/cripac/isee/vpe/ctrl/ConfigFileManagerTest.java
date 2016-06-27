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

import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by ken.yu on 16-10-24.
 */
public class ConfigFileManagerTest {
    @Test
    public void getConfigFileList() throws Exception {
        List<String> cfgFiles =
                ConfigFileManager
                        .getConfigFileList("pedestrian-tracking", "isee-basic")
                        .stream()
                        .map(fileDescriptor -> fileDescriptor.getConcatName())
                        .collect(Collectors.toList());
        for (String file : cfgFiles) {
            System.out.println("Name: " + file);
        }
    }

    @Test
    public void getConcatConfigFilePathList() throws Exception {
        String list = ConfigFileManager.getConcatConfigFilePathList(",");
        System.out.println("List: " + list);
        for (String path : list.split(",")) {
            System.out.println(new BufferedReader(new InputStreamReader(new FileInputStream(path))).readLine());
        }
    }

}