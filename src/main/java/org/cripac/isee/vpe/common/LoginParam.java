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

import com.google.gson.annotations.SerializedName;

import java.io.Serializable;
import java.net.InetAddress;

/**
 * Parameters for server login (e.g. web camera), including camera IP, port, username and password.
 */
public class LoginParam implements Serializable {
    private static final long serialVersionUID = -3831767044437766754L;
    @SerializedName("server-id")
    public ServerID serverID;
    public String username;
    public String password;

    public LoginParam(InetAddress ip, int port, String username, String password) {
        this.serverID = new ServerID(ip, port);
        this.username = username;
        this.password = password;
    }
}
