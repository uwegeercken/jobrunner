/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.datamelt.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;

public class DateTimeUtility
{
	public static final String	VARIABLE_YEAR				= "year";
	public static final String	VARIABLE_MONTH				= "month";
	public static final String	VARIABLE_DAY				= "day";
	public static final String	VARIABLE_HOUR				= "hour";
	public static final String	VARIABLE_MINUTE				= "minute";
	public static final String	VARIABLE_WEEK				= "week";
	
	private static HashMap<String,Integer> map = new HashMap<String,Integer>();
	
	static
	{
		map.put(VARIABLE_YEAR, Calendar.YEAR);
		map.put(VARIABLE_MONTH, Calendar.MONTH);
		map.put(VARIABLE_DAY, Calendar.DAY_OF_MONTH);
		map.put(VARIABLE_HOUR, Calendar.HOUR_OF_DAY);
		map.put(VARIABLE_MINUTE, Calendar.MINUTE);
		map.put(VARIABLE_WEEK, Calendar.WEEK_OF_YEAR);
	}
	
	public static int getFieldValue(int field, int value)
	{
		Calendar calendar = Calendar.getInstance();
		
		calendar.add(field, value);
		return calendar.get(field);
	}
	
	public static int getFieldValue(String name, int offset) throws Exception
	{
		Calendar calendar = Calendar.getInstance();
		
		int translatedField = translateVariableName(name);
		
		if(translatedField!=-1)
		{
			calendar.add(translatedField, offset);
			return calendar.get(translatedField);
		}
		else
		{
			throw new Exception("unknown field: [" + name + "]");
		}
		
	}
	
	public static String getFieldValue(String name, int offset, String dateFormat) throws Exception
	{
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		Calendar calendar = Calendar.getInstance();
		
		int translatedField = translateVariableName(name);
		
		if(translatedField!=-1)
		{
			calendar.add(translatedField, offset);
			return sdf.format(calendar.getTime());
		}
		else
		{
			throw new Exception("unknown field: [" + name + "]");
		}
	}
	
	public static int translateVariableName(String name)
	{
		if(map.containsKey(name))
		{
			return map.get(name);
		}
		else
		{
			return -1;
		}
	}
	
}
