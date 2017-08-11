package org.cripac.isee.vpe.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.collections4.keyvalue.MultiKey;
import org.apache.commons.collections4.map.MultiKeyMap;
import org.cripac.isee.alg.pedestrian.attr.Minute;
import org.cripac.isee.alg.pedestrian.attr.ReIdAttributesTemp;
import org.cripac.isee.vpe.common.SparkStreamingApp;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;

public class PedestrianReIDFeatureEuclideanDistanceSimilarity extends SparkStreamingApp{
	public static final String APP_NAME = "reid-similarity";
	static Logger logger=new ConsoleLogger();
	static GraphDatabaseConnector dbConnector = new Neo4jConnector();
	
	
	public PedestrianReIDFeatureEuclideanDistanceSimilarity(SystemPropertyCenter propCenter) throws Exception{
		super(propCenter, APP_NAME);
	}

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

	@SuppressWarnings("unchecked")
	public static void getSim(List<ReIdAttributesTemp> list) {
		// 前k个
		int k = 3;
		//
		
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
		MultiKeyMap<String, Double> multiKeyMap=new MultiKeyMap<>();
		for (int i = 0; i < list.size(); i++) {
			for (int j = i + 1; j < list.size(); j++) {
//				 System.out.println(String.valueOf(list.get(i)[0])+":"+String.valueOf(list.get(i)[1]));
//				 System.out.println(String.valueOf(list.get(j)[0])+":"+String.valueOf(list.get(j)[1]));
				//目前getCamID是相同的，所以测试时为equals
				if ((list.get(i).getCamID().equals(list.get(j).getCamID()))) {
					if (list.get(i).getFeatureVector()!=null&&list.get(j).getFeatureVector()!=null) {
						
						double sim=getSim(list.get(i).getFeatureVector(), list.get(j).getFeatureVector());
						multiKeyMap.put(list.get(i).getTrackletID(), list.get(j).getTrackletID(),sim);
						
					}
				}
//				disList.add(sim);
			}
//			list.get(i).setEuclideanDistanceSimilarities(map);
//			 System.out.println("--------------");
		}
//		simMaps.add(map);
		List<Entry<MultiKey<? extends String>, Double>> multiKeyMapList=
				new ArrayList<Entry<MultiKey<? extends String>, Double>>(multiKeyMap.entrySet());
		// System.out.println(Math.pow(4,2));;
		// System.out.println(Math.sqrt(8));;

		/*for (int i = 0; i < list.size(); i++) {
			System.out.println(list.get(i));
			
		}*/
		
//		Collections.sort(disList);
//		Collections.sort(list,new ReIdAttributesTempComparator());
		Collections.sort(multiKeyMapList,new Comparator<Entry<MultiKey<? extends String>, Double>>(){
			public int compare(Entry<MultiKey<? extends String>, Double> o1,
					Entry<MultiKey<? extends String>, Double> o2) {
				double sim1Value=o1.getValue().doubleValue();
				double sim2Value=o2.getValue().doubleValue();
				if (sim1Value > sim2Value) {
					return -1;
				} else if (sim1Value == sim2Value) {
					return 0;
				} else {
					return 1;
				}
            }
		});
		/*for (Entry<MultiKey<? extends String>, Double> entry : multiKeyMapList) {
			System.out.println(entry.getValue().doubleValue()+","+entry.getKey().getKey(0)+","+entry.getKey().getKey(1));
		}*/
		
		//前k个，目前为了测试，全部添加
		for (int i = 0; i < multiKeyMapList.size() -(multiKeyMapList.size()-k); i++) {
			Entry<MultiKey<? extends String>, Double> entry=multiKeyMapList.get(i);
			MultiKey<? extends String> keys = entry.getKey();
			double sim = entry.getValue().doubleValue();
			String id1 = keys.getKey(0);
			String id2 = keys.getKey(1);
//			System.out.println(i+"sim:"+sim+",id1:"+id1+",id2:"+id2);
			logger.info(i+",sim:"+sim+",id1:"+id1+",id2:"+id2);
//			dbConnector.addIsGetSim(id1, false);
//			dbConnector.addIsGetSim(id2, false);
			dbConnector.addSimRel(id1, id2, sim);
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
		//目前去掉由minute循环遍历得到Person节点的过程
//		List<Minute> minList=dbConnector.getMinutes();
//		for (int i = 0; i < minList.size(); i++) {
//			System.out.println("minList.get(i):"+i+":"+minList.get(i));
//			List<ReIdAttributesTemp> list=dbConnector.getPedestrianReIDFeatureList(minList.get(i));
			List<ReIdAttributesTemp> list=dbConnector.getPedestrianReIDFeatureList(new Minute());
//			for (int j = 0; j < list.size(); j++) {
//				System.out.println(list.get(j));
//			}
			if (list.size()>0) {
				
//				System.out.println(minList.get(i)+":"+list.size());
				getSim(list);
//			}
		}
	}

	@Override
	public void addToContext() throws Exception {
		// TODO Auto-generated method stub
		
	}

}
