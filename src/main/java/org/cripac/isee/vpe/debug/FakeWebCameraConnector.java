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
package org.cripac.isee.vpe.debug;

import org.cripac.isee.vpe.common.LoginParam;
import org.cripac.isee.vpe.data.WebCameraConnector;
import org.cripac.isee.util.Factory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.InetAddress;
import java.util.Random;

public class FakeWebCameraConnector extends WebCameraConnector {

    public static class FakeWebCameraConnectorFactory implements Factory<WebCameraConnector> {
        private static final long serialVersionUID = -7730008417691845921L;
        LoginParam loginParam;

        public FakeWebCameraConnectorFactory(InetAddress ip, int port, String username, String password) {
            this.loginParam = new LoginParam(ip, port, username, password);
        }

        public FakeWebCameraConnectorFactory(LoginParam loginParam) {
            this.loginParam = loginParam;
        }

        /**
         * Produce a new object.
         *
         * @return An object newly produced.
         * @throws Exception On failure creating a new instance.
         */
        @Nonnull
        @Override
        public WebCameraConnector produce() {
            return new FakeWebCameraConnector(loginParam);
        }
    }

    enum ThreadState {
        UNINITIALIZED,
        RUNNING,
        TERMINATION_REQUESTED,
        TERMINATED
    }

    private PipedOutputStream outputStream;
    private Thread fakeDataGeneratingThread = null;
    private Random random = new Random();
    private ThreadState fakeDataGeneratingThreadState = ThreadState.UNINITIALIZED;

    /**
     * Create a fake web camera connector.
     * It can generate random bytes to form a fake video stream.
     * Call startGeneratingFakeData() to start generating.
     *
     * @param ip       WEBCAM_LOGIN_PARAM of the web camera.
     * @param port     Port opened of the web camera.
     * @param username Username for login.
     * @param password Password for login.
     */
    public FakeWebCameraConnector(InetAddress ip, int port, String username, String password) {
        super(ip, port, username, password);

        outputStream = new PipedOutputStream();
    }

    /**
     * Create a web camera connector.
     *
     * @param loginParam Parameters for login.
     */
    public FakeWebCameraConnector(LoginParam loginParam) {
        super(loginParam);

        outputStream = new PipedOutputStream();
    }


    /**
     * Start generating fake data and fill to the video stream.
     */
    public void startGeneratingFakeData() {
        if (fakeDataGeneratingThread == null) {
            fakeDataGeneratingThread = new Thread(() -> {
                fakeDataGeneratingThreadState = ThreadState.RUNNING;
                try {
                    while (fakeDataGeneratingThreadState == ThreadState.RUNNING) {
                        outputStream.write(random.nextInt());
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    fakeDataGeneratingThreadState = ThreadState.TERMINATED;
                }
            });
            fakeDataGeneratingThread.start();
        }
    }

    /**
     * Stop generating fake data.
     * Note that after stopping, the video stream is still valid,
     * and you can start generating again.
     */
    public void stopGeneratingFakeData() {
        if (fakeDataGeneratingThread != null) {
            fakeDataGeneratingThreadState = ThreadState.TERMINATION_REQUESTED;
            while (fakeDataGeneratingThreadState != ThreadState.TERMINATED) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            fakeDataGeneratingThread = null;
        }
    }

    /**
     * Get the real-time video raw-bit stream from the camera.
     * Subclasses are encouraged to ensure users from different threads
     * can getTracklet correct video stream individually.
     *
     * @return Real-time video raw-bit stream from the connected web camera.
     */
    @Override
    public InputStream getStream() throws IOException {
        return new PipedInputStream(outputStream);
    }
}
