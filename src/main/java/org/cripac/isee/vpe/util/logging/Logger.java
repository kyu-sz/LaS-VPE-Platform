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

import org.apache.log4j.Level;

import javax.annotation.Nonnull;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by ken.yu on 16-10-24.
 */
public abstract class Logger {

    protected Level level;

    protected String localName;

    public Logger(Level level) {
        setLevel(level);

        try {
            localName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            localName = "Unknown Host";
        }
    }

    public void setLevel(Level level) {
        this.level = level;
    }

    public abstract void debug(@Nonnull Object message);

    public abstract void debug(@Nonnull Object message,
                               @Nonnull Throwable t);

    public abstract void info(@Nonnull Object message);

    public abstract void info(@Nonnull Object message,
                              @Nonnull Throwable t);

    public abstract void warn(@Nonnull Object message);

    public abstract void warn(@Nonnull Object message,
                              @Nonnull Throwable t);

    public abstract void error(@Nonnull Object message);

    public abstract void error(@Nonnull Object message,
                               @Nonnull Throwable t);

    public abstract void fatal(@Nonnull Object message);

    public abstract void fatal(@Nonnull Object message,
                               @Nonnull Throwable t);
}
