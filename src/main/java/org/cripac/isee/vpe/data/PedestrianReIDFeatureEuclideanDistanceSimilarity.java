package org.cripac.isee.vpe.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.collections4.keyvalue.MultiKey;
import org.apache.commons.collections4.map.MultiKeyMap;
import org.cripac.isee.alg.pedestrian.attr.Minute;
import org.cripac.isee.alg.pedestrian.attr.ReIdAttributesTemp;
import org.cripac.isee.alg.pedestrian.attr.ReIdAttributesTempComparator;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;

public class PedestrianReIDFeatureEuclideanDistanceSimilarity {
	static Logger logger=new ConsoleLogger();
	static GraphDatabaseConnector dbConnector = new Neo4jConnector();
	public static double getSim(float[] a, float[] b) {
		double distance = 0;

		for (int i = 0; i < a.length; i++) {
			double temp = Math.pow((a[i] - b[i]), 2);
			distance += temp;
		}
		distance = Math.sqrt(distance);
		// System.out.println(distance);
		return 1.0 / (1.0 + distance);
	}

	public static void getSim(List<ReIdAttributesTemp> list) {
		// 前k个
		int k = 10;
		List<ReIdAttributesTemp> disOutList = new ArrayList<>();
//		float[] a = { 1.0f, 2.0f, 3.0f };
//		float[] b = { 4.0f, 5.0f, 6.0f };
//		float[] c = { 8.0f, 8.0f, 9.0f };
//
//		List<float[]> list = new ArrayList<>();
//		list.add(a);
//		list.add(b);
//		list.add(c);
		// double []distances=new double[(new Double(Math.pow(list.size(),
		// 2))).intValue()];
//		List<Double> disList = new ArrayList<>();
//		List<ReIdAttributesTemp> disOutList = new ArrayList<>();
		for (int i = 0; i < list.size(); i++) {
			for (int j = i + 1; j < list.size(); j++) {
//				 System.out.println(String.valueOf(list.get(i)[0])+":"+String.valueOf(list.get(i)[1]));
//				 System.out.println(String.valueOf(list.get(j)[0])+":"+String.valueOf(list.get(j)[1]));
				if (!(list.get(i).getCamID().equals(list.get(j).getCamID()))) {
					
					double sim=getSim(list.get(i).getFeatureVector(), list.get(j).getFeatureVector());
					MultiKeyMap<String, Double> map=new MultiKeyMap<>();
					map.put(list.get(i).getTrackletID(), list.get(j).getTrackletID(),sim);
					list.get(i).setEuclideanDistanceSimilarities(map);
				}
//				disList.add(sim);
			}
//			 System.out.println("--------------");
		}
		// System.out.println(Math.pow(4,2));;
		// System.out.println(Math.sqrt(8));;

//		Collections.sort(disList);
		Collections.sort(list,new ReIdAttributesTempComparator());

		for (int i = list.size() - 1; i >= list.size() ; i--) {
			disOutList.add(list.get(i));
//			 System.out.println(disList.get(i));
		}

		for (int i = 0; i < disOutList.size(); i++) {
			MultiKeyMap<String, Double> map = disOutList.get(i).getEuclideanDistanceSimilarities();
			for (Entry<MultiKey<? extends String>, Double> entry : map.entrySet()) {
				MultiKey<? extends String> keys = entry.getKey();
				double sim = entry.getValue().doubleValue();
				String id1 = keys.getKey(0);
				String id2 = keys.getKey(1);
				System.out.println("sim:"+sim+",id1:"+id1+",id2:"+id2);
//				dbConnector.addIsGetSim(id1, true);
//				dbConnector.addSimRel(id1, id2, sim);
			}
		}
		
	}
	
	public static void main(String[] args) {
		GraphDatabaseConnector dbConnector = new Neo4jConnector();
//        List<String> list=dbConnector.getPedestrianReIDFeatureBase64List("dli_test");
//        List<ReIdAttributesTemp> list=dbConnector.getPedestrianReIDFeatureList("dli_test");
//        List<float[]> list=dbConnector.getPedestrianReIDFeatureList(true,false);
//        for (Iterator iterator = list.iterator(); iterator.hasNext();) {
//        	ReIdAttributesTemp fs = (ReIdAttributesTemp) iterator.next();
//			System.out.println(fs.toString());
//		}
		List<Minute> minList=dbConnector.getMinutes();
		for (int i = 0; i < minList.size(); i++) {
//			System.out.println("minList.get(i):"+i+":"+minList.get(i));
			List<ReIdAttributesTemp> list=dbConnector.getPedestrianReIDFeatureList(minList.get(i));
//			for (int j = 0; j < list.size(); j++) {
//				System.out.println(list.get(j));
//			}
			if (list.size()>0) {
				
//				System.out.println(minList.get(i)+":"+list.size());
				getSim(list);
			}
		}
	}

}
