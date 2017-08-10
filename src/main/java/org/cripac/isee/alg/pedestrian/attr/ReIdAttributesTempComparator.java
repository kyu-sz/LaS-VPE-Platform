package org.cripac.isee.alg.pedestrian.attr;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections4.keyvalue.MultiKey;
import org.apache.commons.collections4.map.MultiKeyMap;

public class ReIdAttributesTempComparator implements Comparator {

	@Override
	public int compare(Object o1, Object o2) {
		// TODO Auto-generated method stub
		ReIdAttributesTemp reIdAttributesTemp1=null;
		ReIdAttributesTemp reIdAttributesTemp2=null;
		if (o1!=null&&o2!=null) {
			
			reIdAttributesTemp1=(ReIdAttributesTemp)o1;
			reIdAttributesTemp2=(ReIdAttributesTemp)o2;
		}
		
		MultiKeyMap<String,Double>  reIdAttributesTempMap1=reIdAttributesTemp1.getEuclideanDistanceSimilarities();
		MultiKeyMap<String,Double>  reIdAttributesTempMap2=reIdAttributesTemp2.getEuclideanDistanceSimilarities();
		String sim1id1=null,sim1id2=null,sim2id1=null,sim2id2=null,camID;
		double sim1Value=0.0,sim2Value=0.0;
		if (reIdAttributesTempMap1!=null&&reIdAttributesTempMap2!=null) {
			for (Entry<MultiKey<? extends String>, Double> entry : reIdAttributesTempMap1.entrySet()) {
				MultiKey<? extends String> sim1Keys=entry.getKey();
				sim1Value=entry.getValue().doubleValue();
				sim1id1=sim1Keys.getKey(0);
				sim1id2=sim1Keys.getKey(1);
			}
			for (Entry<MultiKey<? extends String>, Double> entry : reIdAttributesTempMap2.entrySet()) {
				MultiKey<? extends String> sim2Keys=entry.getKey();
				sim2Value=entry.getValue().doubleValue();
				sim2id1=sim2Keys.getKey(0);
				sim2id2=sim2Keys.getKey(1);
			}
		}
		if (sim1id1!=null&&sim2id1!=null) {
			
		}
		if(sim1Value>sim2Value){
			return 1;
		}else if (sim1Value==sim2Value) {
			return 0;
		}else {
			return -1;
		}
	}
	
}
