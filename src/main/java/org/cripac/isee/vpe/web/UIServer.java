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

package org.cripac.isee.vpe.web;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;

/**
 * Created by ken.yu on 16-10-22.
 * <p>
 * The class UIServer is for receiving HTTP requests from Web UI, containing
 * user-specified tasks and queries.
 *
 * @author ken.yu
 */
public class UIServer {

    public final static int PORT = 9100;

    private Vertx vertx;
    private HttpServer server;

    /**
     * Create a server.
     */
    public UIServer() {
        vertx = Vertx.vertx();
        server = vertx.createHttpServer();

        server.requestHandler(request -> {
            // This handler gets called for each request that arrives on the server
            HttpServerResponse response = request.response();
            response.putHeader("content-INPUT_TYPE", "text/plain");

            // Write to the response and end it
            response.end("Hello World!");
        });
    }

    /**
     * Stop listening.
     */
    public void close() {
        server.close();
    }

    /**
     * Start listening.
     */
    public void listen(int port) {
        server.listen(port);
    }

    public void main(String[] args) {
        UIServer server = new UIServer();
        server.listen(PORT);
    }
}
