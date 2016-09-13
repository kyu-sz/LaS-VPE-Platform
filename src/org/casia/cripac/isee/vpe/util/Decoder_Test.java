package org.cripac.isee.vpe.util;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.cripac.isee.vpe.util.Decoder.VideoInfo;

public class Decoder_Test {
	public static void main(String[] args) {
		try {
			byte[] data = null;
			String local = "/home/zy/Documents/VPE/videosource/20140211174011-20140211175523.h264";//
			// String hdfs="hdfs://rman-nod1:8020/mnt/disk1/zhouyang/video/";//testforrtask3.mp4
			Configuration conf = new Configuration();
			// FileSystem fileS=FileSystem.get(new URI(hdfs), conf);
			FileSystem localFile = FileSystem.getLocal(conf);
			Path input = new Path(local);
			FileStatus[] inputFile = localFile.listStatus(input);

			// FSDataInputStream localInStream=.open(input);
			// FSDataOutputStream outStream=fileS.create(output);
			for (int i = 0; i < inputFile.length; i++) {
				System.out.println(inputFile[i].getPath().getName());
				// hdfs=hdfs+inputFile[i].getPath().getName();
				// Path out=new Path(hdfs);
				FSDataInputStream in = localFile.open(inputFile[i].getPath());
				// FSDataOutputStream outStream=fileS.create(out);
				ByteArrayOutputStream output = new ByteArrayOutputStream();
				byte[] buffer = new byte[1024];
				int bytesRead = 0;
				while ((bytesRead = in.read(buffer)) != -1) {
					// System.out.println(buffer);
					// outStream.write(buffer,0,bytesRead);
					output.write(buffer, 0, bytesRead);
				}
				data = output.toByteArray();
				in.close();

				// VideoData decodedFrames = Download.decode(data.length, data);
				Decoder decoder = new Decoder(data);
				VideoInfo info = decoder.getVideoInfo();

				System.out.println("-------------------------------");
				System.out.printf("%d %d %d\n", info.width, info.height, info.channels);
				System.out.println("-------------------------------");

				System.out.println("-------------------------------");
				byte[] frame = new byte[info.channels * info.height * info.width];
				int frame_num = 0;
				for (int z = 0; z < 10; ++z) {
					System.arraycopy(decoder.nextFrame(), 0, frame, 0, info.channels * info.height * info.width);

					System.out.println("Jave received:");
					for (int j = 0; j < 50; ++j) {
						System.out.print(frame[j + info.channels * info.height * info.width / 2 - 25]);
					}
					System.out.println();
				}
				/*
				 * while(frame != NULL){
				 * 
				 * }
				 */
			}
		} catch (FileNotFoundException ex1) {
			ex1.printStackTrace();
		} catch (IOException ex1) {
			ex1.printStackTrace();
		}
	}
}
