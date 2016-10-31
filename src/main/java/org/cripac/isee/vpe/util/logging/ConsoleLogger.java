package org.cripac.isee.vpe.util.logging;/***********************************************************************
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
import java.io.Serializable;

/**
 * Created by ken.yu on 16-10-24.
 */
public class ConsoleLogger extends Logger implements Serializable {
    @Override
    public void debug(@Nonnull Object message) {
        System.out.println(message);
    }

    @Override
    public void debug(@Nonnull Object message,
                      @Nonnull Throwable t) {
        System.out.println(message);
        System.out.println(t.getStackTrace());
    }

    @Override
    public void info(@Nonnull Object message) {
        System.out.println(message);
    }

    @Override
    public void info(@Nonnull Object message,
                     @Nonnull Throwable t) {
        System.out.println(message);
        System.out.println(t.getStackTrace());
    }

    @Override
    public void error(@Nonnull Object message) {
        System.err.println(message);
    }

    @Override
    public void error(@Nonnull Object message,
                      @Nonnull Throwable t) {
        System.err.println(message);
        System.out.println(t.getStackTrace());
    }

    @Override
    public void fatal(@Nonnull Object message) {
        System.err.println(message);
    }

    @Override
    public void fatal(@Nonnull Object message,
                      @Nonnull Throwable t) {
        System.err.println(message);
        System.out.println(t.getStackTrace());
    }
}
