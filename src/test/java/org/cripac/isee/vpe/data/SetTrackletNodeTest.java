/**
 * To test the function getTrackletInfo in HadoopHelper
 * 
 * @Author da.li on 2017-04-17
 */

package org.cripac.isee.vpe.data;

import java.util.Iterator;
import java.util.List;

import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;

public class SetTrackletNodeTest {

    public static void main(String[] args) throws Exception {
     
    	Logger logger=new ConsoleLogger();
        // It is necessary to change the directory as your practical situation.
        String storeDir = "/user/labadmin/metadata/20131223102739-20131223103331/280f5b30-1149-4dfa-9462-12fd8f307edc.har/9";
//        Logger logger=new SynthesizedLogger(APP_NAME, propCenter);
        GraphDatabaseConnector dbConnector = new Neo4jConnector();
        long startTime = System.currentTimeMillis();
//        dbConnector.setTrackletSavingPath("test_1a", storeDir);
//        List<String> list=dbConnector.getPedestrianReIDFeatureBase64List("dli_test");
      dbConnector.delNode("CAM01-20140226110701-20140226111249_tarid113");
//        dbConnector.addIsFinish("/user/vpe.cripac/new2/20131223141032-20131223141628/d3fba52d-c1a5-4aa0-8c95-6be46892688f/2", true);
//        dbConnector.addIsGetSim("/user/vpe.cripac/new2/20131223141032-20131223141628/d3fba52d-c1a5-4aa0-8c95-6be46892688f/0", false);
//        dbConnector.addSimRel("/user/vpe.cripac/new2/20131223141032-20131223141628/d3fba52d-c1a5-4aa0-8c95-6be46892688f/2", "/user/vpe.cripac/new2/20131223141032-20131223141628/d3fba52d-c1a5-4aa0-8c95-6be46892688f/0", 1.0);
//        for (Iterator iterator = list.iterator(); iterator.hasNext();) {
//			String string = (String) iterator.next();
//			System.out.println(string);
//			logger.info(string);
//		}
        long endTime = System.currentTimeMillis();
//        System.out.println("Cost time of insert a node: " + (endTime - startTime) + "ms");
        logger.info("Cost time of insert a node: " + (endTime - startTime) + "ms");
    }
}
