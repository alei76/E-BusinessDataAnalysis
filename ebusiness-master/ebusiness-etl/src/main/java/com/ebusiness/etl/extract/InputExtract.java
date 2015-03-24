package com.ebusiness.etl.extract;


/**
 * 
 * @author panzhichun
 *
 */
public class InputExtract implements IInputExtract {

	
	
	
	
	/**
	 * 产生核心数据，不包含任何格式处理，格式处理由其子类负责
	 */
	public String extract(String input) {
		return input;
	}
     
}
