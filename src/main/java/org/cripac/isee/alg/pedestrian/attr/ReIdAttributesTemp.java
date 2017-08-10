/*
 * This file is part of LaS-VPE Platform.
 *
 * LaS-VPE Platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * LaS-VPE Platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with LaS-VPE Platform.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cripac.isee.alg.pedestrian.attr;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map.Entry;

import org.apache.avro.ipc.trace.SingletonTestingTracePlugin;
import org.apache.commons.collections4.keyvalue.MultiKey;
import org.apache.commons.collections4.map.MultiKeyMap;

import com.google.gson.Gson;

/**
 * The Attribute class stores all the pre-defined attributes of a pedestrian at
 * one moment in a track. In other words, each attribute object correspond to
 * one bounding box in a track.
 * <p>
 * TODO Fill the pre-defined attributes. </p>
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class ReIdAttributesTemp implements Serializable {


    private String camID;
    private String trackletID;
//    private String reidFeature;
    private Long startTime;
    private float[] featureVector;
    
    
    MultiKeyMap<String,Double>  euclideanDistanceSimilarities;

	

	public String getTrackletID() {
		return trackletID;
	}

	public void setTrackletID(String trackletID) {
		this.trackletID = trackletID;
	}

	public String getCamID() {
		return camID;
	}

	public void setCamID(String camID) {
		this.camID = camID;
	}

	


	public float[] getFeatureVector() {
		return featureVector;
	}

	public void setFeatureVector(float[] featureVector) {
		this.featureVector = featureVector;
	}

	public Long getStartTime() {
		return startTime;
	}

	public void setStartTime(Long startTime) {
		this.startTime = startTime;
	}
	
	
	
	public MultiKeyMap<String, Double> getEuclideanDistanceSimilarities() {
		return euclideanDistanceSimilarities;
	}

	public void setEuclideanDistanceSimilarities(MultiKeyMap<String, Double> euclideanDistanceSimilarities) {
		this.euclideanDistanceSimilarities = euclideanDistanceSimilarities;
	}

	@Override
	public String toString() {
		return new Gson().toJson(this);
	}
	
	
	public static void main(String[] args) {
		ReIdAttributesTemp reIdAttributesTemp=new ReIdAttributesTemp();
		MultiKeyMap<String, Double> euclideanDistanceSimilarities =new MultiKeyMap<>();
		euclideanDistanceSimilarities.put("a","b", 30.0);
		reIdAttributesTemp.setEuclideanDistanceSimilarities(euclideanDistanceSimilarities);
		System.out.println(reIdAttributesTemp.getEuclideanDistanceSimilarities());
		
			for (Entry<MultiKey<? extends String>, Double> entryOut : euclideanDistanceSimilarities.entrySet()) {
				MultiKey<? extends String> sim1Keys= entryOut.getKey();
				Double sim1=entryOut.getValue();
//				System.out.println(sim1Keys.getKeys());
//				System.out.println(sim1);\
				String s=sim1Keys.getKey(1);
				System.out.println(s);;
//				String key1=new String(sim1Keys.getKeys()[0]);
//				String key2=new String(sim1Keys.getKeys()[1]);
//				System.out.println(key1+" "+key2+" "+sim1);
		}
		double a=0.0,b=0.0;
		System.out.println(a+" "+b);
	}

    
}
