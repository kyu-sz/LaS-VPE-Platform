/**
 * This file is part of LaS-VPE Platform.
 * <p>
 * LaS-VPE Platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * LaS-VPE Platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with LaS-VPE Platform.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
 * Created by ken.yu on 16-10-9.
 */

/**
 * Created by ken.yu on 16-10-9.
 */

package org.cripac.isee.vpe.util.tracking;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * The class DirectoryHierarchy is used to store the hierarchy of a directory
 * as well as the files the directory directly contains.
 */
public class DirectoryHierarchy {
    private String name;
    private Map<String, DirectoryHierarchy> subHierarchy = new HashMap<>();
    private Set<String> files = new HashSet<>();
    private DirectoryHierarchy parent = null;

    public DirectoryHierarchy getSubHierarchy(@Nonnull String name) {
        return subHierarchy.get(name);
    }

    /**
     * Add a collection of files into this hierarchy.
     *
     * @param files A collection of names of the files.
     */
    public void addFiles(@Nonnull Collection<String> files) {
        this.files.addAll(files);
    }

    /**
     * Add into this group all the files in the folder
     * whose NAME matches a regex.
     *
     * @param regex The regex the added files' NAME should match.
     */
    public void addFiles(@Nonnull String regex) {
        File folder = new File(getPath());
        String[] matchedFiles = folder.list(
                (file, s) -> Pattern.compile(regex).matcher(s).matches());
        if (matchedFiles != null) {
            addFiles(Arrays.asList(matchedFiles));
        }
    }

    /**
     * Create a top hierarchy.
     *
     * @param name The NAME of the top hierarchy.
     * @return The top hierarchy.
     */
    public static DirectoryHierarchy createTop(@Nonnull String name) {
        return new DirectoryHierarchy(name);
    }

    /**
     * Create a hierarchy given NAME and parent hierarchy.
     *
     * @param name   The NAME of the hierarchy.
     * @param parent The NAME of parent hierarchy.
     */
    public DirectoryHierarchy(@Nonnull String name,
                              @Nullable DirectoryHierarchy parent) {
        this.name = name;
        this.parent = parent;
        parent.subHierarchy.put(name, this);
    }

    /**
     * Gether all the files under this group recursively.
     *
     * @return An array list of gathered files.
     */
    public List<FileDescriptor> gatherFiles() {
        return wrapUpperHierarchies(gatherLowerFiles());
    }

    private List<FileDescriptor> wrapUpperHierarchies(List<FileDescriptor> list) {
        if (parent == null) {
            return list;
        } else {
            return parent.wrapUpperHierarchies(list.stream()
                    .map(descriptor -> descriptor.wrap(name))
                    .collect(Collectors.toList()));
        }
    }

    private List<FileDescriptor> gatherLowerFiles() {
        // Create a list with descriptors of files in current hierarchy.
        List<FileDescriptor> gathered = files.stream()
                .map(FileDescriptor::new)
                .collect(Collectors.toList());

        // Add files in sub-hierarchies after wrapping them.
        for (DirectoryHierarchy subgroup : subHierarchy.values()) {
            gathered = subgroup.gatherLowerFiles().stream()
                    .map(fileDescriptor -> fileDescriptor.wrap(subgroup.name))
                    .collect(Collectors.toList());
        }

        return gathered;
    }

    private DirectoryHierarchy(@Nonnull String name) {
        this.name = name;
        this.parent = null;
    }

    private String getPath() {
        if (parent != null) {
            return parent.getPath() + "/" + name;
        } else {
            return name;
        }
    }
}