package org.cripac.isee.vpe.data;

import java.util.HashMap;
import java.util.Map;

import org.cripac.isee.vpe.common.SparkStreamingApp;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.util.logging.Logger;

public class AddAppTest extends SparkStreamingApp{

	public static final String APP_NAME = "AddAppTest";
	public AddAppTest(SystemPropertyCenter propCenter) throws Exception {
		super(propCenter, APP_NAME);
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		SystemPropertyCenter propCenter = new SystemPropertyCenter(args);

        SparkStreamingApp app = new AddAppTest(propCenter);
        app.initialize();
        app.start();
        app.awaitTermination();
	}

	@Override
	public void addToContext() throws Exception {
		// TODO Auto-generated method stub
		final Logger logger = loggerSingleton.getInst();
		logger.info(APP_NAME);
		logger.info("app add test 1232432431243");
//		Map<String, String> map=new HashMap<String, String>();
//		map.p
	}

}
