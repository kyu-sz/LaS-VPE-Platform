package org.cripac.isee.vpe.common;/*
 * This file is part of las-vpe-platform.
 *
 * las-vpe-platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * las-vpe-platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with las-vpe-platform. If not, see <http://www.gnu.org/licenses/>.
 *
 * Created by ken.yu on 17-3-10.
 */

import java.util.ArrayList;
import java.util.Iterator;
import java.util.function.Consumer;

public class ParallelExecutor {
    public static <T> void execute(Iterator<T> items, Consumer<T> consumer) {
        execute(items, Runtime.getRuntime().availableProcessors(), consumer);
    }

    public static <T> void execute(Iterator<T> items, int parallelism, Consumer<T> consumer) {
        assert parallelism >= 1;
        ArrayList<T> dataBuf = new ArrayList<>(parallelism);
        while (items.hasNext()) {
            for (int i = 0; i < parallelism; ++i) {
                dataBuf.set(i, items.hasNext() ? items.next() : null);
            }
            dataBuf.parallelStream().forEach(consumer);
        }
    }
}
