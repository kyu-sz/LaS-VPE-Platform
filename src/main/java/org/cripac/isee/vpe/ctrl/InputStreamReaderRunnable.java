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

package org.cripac.isee.vpe.ctrl;

import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The InputStreamReaderRunnable class is designed for running a thread
 * continuously reading from an input stream then display to the console.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class InputStreamReaderRunnable implements Runnable {
    @Nonnull
    private final Logger logger;
    @Nonnull
    private BufferedReader reader;
    @Nonnull
    private String name;
    private boolean isErrStream = false;
    @Nonnull
    private final AtomicReference<Boolean> running;

    /**
     * Create a thread to read from a input stream and print it to the console.
     *
     * @param is   The input stream to read.
     * @param name The NAME of the stream.
     */
    public InputStreamReaderRunnable(@Nonnull Logger logger,
                                     @Nonnull InputStream is,
                                     @Nonnull String name,
                                     @Nonnull AtomicReference<Boolean> running) {
        this.logger = logger;
        this.reader = new BufferedReader(new InputStreamReader(is));
        this.name = name;
        isErrStream = name.toLowerCase().contains("error");
        this.running = running;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        try {
            String line = reader.readLine();
            while (running.get() && line != null) {
                if (isErrStream) {
                    logger.error(line);
                } else {
                    logger.info(line);
                }
                line = reader.readLine();
            }
            reader.close();
            logger.info(name + " stream exiting.");
        } catch (IOException e) {
            logger.error(name + " stream encountered exception", e);
        }
    }

}
