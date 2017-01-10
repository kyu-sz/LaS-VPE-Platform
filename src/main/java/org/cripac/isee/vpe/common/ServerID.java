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
package org.cripac.isee.vpe.common;

import java.io.Serializable;
import java.net.InetAddress;

/**
 * Identifier of external servers.
 *
 * Created by ken.yu on 16-12-2.
 */
public class ServerID implements Serializable {
    private static final long serialVersionUID = 2091306632553954507L;
    public InetAddress address;
    public int port;

    public ServerID(InetAddress address, int port) {
        this.address = address;
        this.port = port;
    }

    @Override
    public String toString() {
        return address + ":" + port;
    }
}
