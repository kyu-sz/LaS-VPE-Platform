package org.cripac.isee.vpe.debug;

import org.cripac.isee.pedestrian.attr.Attributes;
import org.cripac.isee.pedestrian.attr.ExternPedestrianAttrRecognizer;

import javax.annotation.Nonnull;
import java.net.Inet4Address;
import java.net.InetAddress;

/**
 * Created by zy on 12/26/16.
 */


public class FakeExternAttrRecog {

    public ExternPedestrianAttrRecognizer test;


    public void Initial(String sovlverAddress,int port){
        try{
            //test = new ExternPedestrianAttrRecognizer(Inet4Address.getByName(sovlverAddress),port);
        }
        catch (Exception e) {
            System.out.println("Initialization Failed.");
        }
    }
    public void main(){
        Initial("172.18.33.90",8500);

    }
}
