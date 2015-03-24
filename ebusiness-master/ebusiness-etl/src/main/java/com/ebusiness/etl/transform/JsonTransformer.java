package com.ebusiness.etl.transform;

import java.util.HashMap;

import com.ebusiness.etl.extract.BracketInputExtract;
import com.ebusiness.etl.extract.InputExtract;
import com.ebusiness.etl.extract.QuoteInputExtract;


/**
 * json格式的转换器
 * @author panzhichun
 *
 */
public class JsonTransformer implements ITransformer {

	
	
	public HashMap<String, String> getFormatData(String value) {
        String[] valueArray = new QuoteInputExtract(new BracketInputExtract(new InputExtract())).extract(value).split(",|:");
        //减少重哈希，提高效率，但是加大了内存的消耗
        HashMap<String, String> dataContainer =  new HashMap<String, String>((int) Math.ceil(valueArray.length/2/0.7)) ;
		dataContainer.put(valueArray[0].trim(), valueArray[1].trim());
		dataContainer.put(valueArray[2].trim(), valueArray[3].trim());
		dataContainer.put(valueArray[4].trim(), valueArray[5].trim());
		dataContainer.put(valueArray[6].trim(), valueArray[7].trim());
		dataContainer.put(valueArray[8].trim(), valueArray[9].trim());
		dataContainer.put(valueArray[10].trim(), valueArray[11].trim());
		dataContainer.put(valueArray[12].trim(), valueArray[13].trim());
		return dataContainer;
	}

	

}
