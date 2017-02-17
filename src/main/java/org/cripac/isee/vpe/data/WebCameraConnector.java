/*
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
 */

package org.cripac.isee.vpe.data;

import org.cripac.isee.vpe.common.LoginParam;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;

/**
 * The WebCameraConnector is the base class for all web camera connector classes.
 * It is not responsible for decoding the stream.
 * <p>
 * Created by ken.yu on 16-12-2.
 */
public abstract class WebCameraConnector {

    protected LoginParam loginParam;

    /**
     * Create a web camera connector.
     *
     * @param ip       WEBCAM_LOGIN_PARAM of the web camera.
     * @param port     Port opened of the web camera.
     * @param username Username for login.
     * @param password Password for login.
     */
    public WebCameraConnector(InetAddress ip, int port, String username, String password) {
        this.loginParam = new LoginParam(ip, port, username, password);
    }

    /**
     * Create a web camera connector.
     *
     * @param loginParam Parameters for login.
     */
    public WebCameraConnector(LoginParam loginParam) {
        this.loginParam = loginParam;
    }

    /**
     * Get the real-time video raw-bit stream from the camera.
     * Subclasses are encouraged to ensure users from different threads
     * can get correct video stream individually.
     *
     * @return Real-time video raw-bit stream from the connected web camera.
     */
    public abstract InputStream getStream() throws IOException;
}
