package org.cripac.isee.alg.pedestrian.attr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.cripac.isee.alg.pedestrian.tracking.Tracklet;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet.BoundingBox;
import org.cripac.isee.util.Singleton;
import org.cripac.isee.vpe.alg.pedestrian.attr.PedestrianAttrRecogApp.AppPropertyCenter;
import org.cripac.isee.vpe.util.logging.Logger;

public class DeepMARCaffe2NativeMul  implements DeepMARCaffe2, BatchRecognizer{
	
	List<DeepMARCaffe2Native> deepMARCaffe2NativeList=new ArrayList<>();
	
	public DeepMARCaffe2NativeMul(AppPropertyCenter propCenter,Singleton<Logger> loggerSingleton){
		Collection<Integer> gpus=propCenter.gpus;
		gpus.forEach(f->{
			System.out.println("当前gpu是："+f);
		
			try {
				DeepMARCaffe2Native deepMARCaffe2Native=new DeepMARCaffe2Native(String.valueOf(f), loggerSingleton.getInst());
				deepMARCaffe2NativeList.add(deepMARCaffe2Native);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
	}

	@Override
	public Attributes recognize(Tracklet tracklet) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Attributes[] recognize(BoundingBox[] bboxes) {
		// TODO Auto-generated method stub
		return null;
	}
	
	
}
