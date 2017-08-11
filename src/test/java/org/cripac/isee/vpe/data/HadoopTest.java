package org.cripac.isee.vpe.data;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopTest {

	private static final Logger log = LoggerFactory.getLogger(HadoopTest.class);
	private static Configuration conf;
	private static FileSystem fs;

	public static FileSystem HDFSOperation() throws IOException {
		conf = new Configuration();
		conf.set("fs.default.name", "hdfs://172.18.33.84:8020");
		String hadoopHome = System.getenv("HADOOP_HOME");
		conf.addResource(new Path(hadoopHome + "/etc/hadoop/core-site.xml"));
		conf.addResource(new Path(hadoopHome + "/etc/hadoop/yarn-site.xml"));
		conf.setBoolean("dfs.support.append", true);
		
		fs = FileSystem.get(conf);
		return fs;
	}

	public static boolean downLoadFromHdfs(String hdfsPath, String localPath) throws IOException, URISyntaxException {
		boolean bFlg = false;
		// FileSystem fs = FileSystem.get(conf);//这里的conf已经配置的ip地址和端口号
		FileSystem fs = HDFSOperation();
		InputStream in = null;
		OutputStream out = null;
		try {
			in = fs.open(new Path(hdfsPath));
			out = new FileOutputStream(localPath);
			IOUtils.copyBytes(in, out, 4096, true);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
			fs.close();
		}
		return bFlg;
	}

	public static void main(String[] args) throws Exception {

		String dsf = "/user/vpe.cripac/new2/20131223103919-20131223104515/46b64a7f-fbbb-406b-9892-f47c6c8d93b2/27/bbox.data";
		downLoadFromHdfs(dsf, "/home/vpe.cripac/projects/jun.li/KafkaTest2/2.txt");

	}
}