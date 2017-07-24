package org.cripac.isee.vpe.util;

import java.io.IOException;
import java.net.URISyntaxException;

import org.cripac.isee.alg.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.util.hdfs.HadoopHelper;

public class GetTracklet {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		String base = "har://hdfs-kman-nod2:8020";
        String storeDir = "/user/vpe.cripac/new2/20131223125419-20131223130015/bbb12b39-426e-4c99-9188-828a6ce1de0b/46";
//        storeDir = base + storeDir;

        try {
            Tracklet tracklet = HadoopHelper.retrieveTracklet(storeDir);
            System.out.println("Test getTrackletInfo, the info is:");
            System.out.println(tracklet.toString());
        } catch(IOException e1) {
        } catch(URISyntaxException e2) {
        }
	}
	
}
