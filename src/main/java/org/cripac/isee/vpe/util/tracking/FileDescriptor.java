package org.cripac.isee.vpe.util.tracking;/***********************************************************************
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

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.ListIterator;

/**
 * The FileDescriptor stores the folder hierarchy and NAME
 * of a file.
 */
public class FileDescriptor {
    /**
     * The hierarchy of folders containing this configuration file.
     */
    private ArrayList<String> hierarchy = new ArrayList<>();
    /**
     * The NAME of the configuration file.
     */
    private String name;

    /**
     * Create a configuration file descriptor with its NAME.
     *
     * @param name The NAME of the configuration file.
     */
    public FileDescriptor(@Nonnull String name) {
        this.name = name;
    }

    /**
     * Indicate that the given folder further contains the containing
     * folder hierarchy of this configuration file.
     *
     * @param folderName The NAME of the outer folder.
     * @return The descriptor itself.
     */
    public FileDescriptor wrap(@Nonnull String folderName) {
        this.hierarchy.add(folderName);
        return this;
    }

    /**
     * Get the path of this configuration file relative to the most upper
     * folder known to the descriptor.
     *
     * @return The relative path of this configuraiton file.
     */
    public String getPath() {
        return concat("/");
    }

    /**
     * Get the concatenated NAME of this configuration file containing
     * all the information of the hierarchy.
     *
     * @return The concatenated NAME.
     */
    public String getConcatName() {
        return concat("-");
    }

    /**
     * Concatenate the hierarchy folders with the NAME
     * with a given connector.
     *
     * @param connector The connector for concatenating.
     * @return A concatenated string.
     */
    private String concat(@Nonnull String connector) {
        StringBuilder concat = new StringBuilder();
        ListIterator listIterator = hierarchy.listIterator(hierarchy.size());
        while (listIterator.hasPrevious()) {
            String dir = (String) listIterator.previous();
            concat.append(dir + connector);
        }
        concat.append(name);
        return concat.toString();
    }
}
