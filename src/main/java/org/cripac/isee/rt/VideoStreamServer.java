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

package org.cripac.isee.rt;

import org.apache.commons.io.IOUtils;
import org.cripac.isee.vpe.alg.PedestrianTrackingApp;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * The class VideoStreamServer is responsible for responding to request of
 * inputting real-time video streams, and transform video streams into
 * fragment streams so as to send them to Kafka.
 * <p>
 * Created by ken.yu on 16-12-2.
 */
public class VideoStreamServer {

    public final static int PORT = 9101;

    final ServerSocket serverSocket;

    public VideoStreamServer() throws IOException {
        serverSocket = new ServerSocket(PORT);
    }

    /**
     * The ServerThread is a thread that receives real-time video
     * stream from a camera socket, then find an idle tracking worker
     * and forward the stream to it.
     */
    public class ServerThread implements Runnable {

        private Socket client = null;

        public ServerThread(Socket client) {
            this.client = client;
        }

        @Override
        public void run() {
            InputStream inputStream;
            try {
                inputStream = client.getInputStream();
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }


        }
    }

    public void run() throws IOException {
        while (true) {
            Socket socket = serverSocket.accept();
            Thread thread = new Thread(new ServerThread(socket));
            thread.start();
        }
    }

    public static void main(String[] args) {
        try {
            VideoStreamServer server = new VideoStreamServer();
            server.run();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
