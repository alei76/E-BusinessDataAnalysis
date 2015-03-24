package com.ebusiness.etl.transform;

import java.util.HashMap;

public interface ITransformer {
	
	
	/**
	 * 获取格式化后的数据
	 * @return
	 */
	public HashMap<String,String> getFormatData(String value);
}
