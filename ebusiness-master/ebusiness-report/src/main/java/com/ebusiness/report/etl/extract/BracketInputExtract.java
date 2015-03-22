package com.ebusiness.report.etl.extract;


/**
 * 对InputExtract增强，实现具体的格式化操作,去掉输入字符串中的大括号
 * 
 * @author panzhichun
 *
 */
public class BracketInputExtract extends InputExtract {

	InputExtract component;

	public BracketInputExtract(InputExtract c) {
		component = c;
	}

	@Override
	public String extract(String input) {
		String value = component.extract(input);
		value = value.replaceAll("[{*}]", "");
		return value;
	}

}
