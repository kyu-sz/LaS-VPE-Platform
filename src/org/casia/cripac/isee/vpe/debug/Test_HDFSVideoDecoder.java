/*This is a test for jni: vpe_decoder*/
package org.casia.cripac.isee.vpe.debug;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;

import org.casia.cripac.isee.vpe.common.HDFSVideoDecoder;
import org.casia.cripac.isee.vpe.common.VideoData;

/**
 * @author : Yang Zhou
 */
public class Test_HDFSVideoDecoder extends HDFSVideoDecoder {

	private native VideoData videodecoder(int buffer_len, byte[] fsbuffer);

	public VideoData decode(String videoURL) {
		String local = "";
		VideoData data = new VideoData();
		Configuration conf = new Configuration();

		try {
			byte[] fsbuffer;
			FileSystem localFile = FileSystem.getLocal(conf);
			Path input = new Path(local);
			FileStatus[] inputFile = localFile.listStatus(input);

			for (int i = 0; i < inputFile.length; i++) {
				System.out.println(inputFile[i].getPath().getName());

				FSDataInputStream in = localFile.open(inputFile[i].getPath());
				ByteArrayOutputStream output = new ByteArrayOutputStream();
				byte[] buffer = new byte[1024];
				int bytesRead = 0;
				while ((bytesRead = in.read(buffer)) != -1) {
					output.write(buffer, 0, bytesRead);
				}
				fsbuffer = output.toByteArray();
				in.close();
				int buffer_len = fsbuffer.length;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}
}
