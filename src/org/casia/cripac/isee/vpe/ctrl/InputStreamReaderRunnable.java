/***********************************************************************
 * This file is part of VPE-Platform.
 * 
 * VPE-Platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * VPE-Platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with VPE-Platform.  If not, see <http://www.gnu.org/licenses/>.
 ************************************************************************/
package org.casia.cripac.isee.vpe.ctrl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * The InputStreamReaderRunnable class is designed for running a thread
 * continuously reading from an input stream then display to the console.
 * 
 * @author Ken Yu, CRIPAC, 2016
 */
public class InputStreamReaderRunnable implements Runnable {

	private BufferedReader reader;

	private String name;
	private boolean errorStream = false;

	/**
	 * Create a thread to read from a input stream and print it to the console.
	 * 
	 * @param is
	 *            The input stream to read.
	 * @param name
	 *            The name of the stream.
	 */
	public InputStreamReaderRunnable(InputStream is, String name) {
		this.reader = new BufferedReader(new InputStreamReader(is));
		this.name = name;
		errorStream = name.toLowerCase().contains("error");
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
			while (line != null) {
				if (errorStream) {
					System.err.println("|" + name + "|" + line);
				} else {
					System.out.println("|" + name + "|" + line);
				}
				line = reader.readLine();
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
