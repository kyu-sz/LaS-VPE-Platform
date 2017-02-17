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

package org.cripac.isee.vpe.util;

import javax.annotation.Nonnull;
import java.io.Serializable;

/**
 * The class Factory can produce new objects in a manner fixed once initialized.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public interface Factory<T> extends Serializable {
    /**
     * Produce a new object.
     *
     * @return An object newly produced. Returns null on failure.
     */
    @Nonnull
    T produce() throws Exception;
}
