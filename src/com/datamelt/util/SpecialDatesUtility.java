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
import java.util.ArrayList;
import java.util.Calendar;

public class SpecialDatesUtility
{
	
	public static final String METHOD_WEEK_MONDAY 			= "weekMonday";
	public static final String METHOD_WEEK_SUNDAY 			= "weekSunday";
	public static final String METHOD_MONTH_LAST_DAY		= "monthLastDay";
	public static final String METHOD_MONTH_FIRST_DAY		= "monthFirstDay";
	
	private static int firstDayOfWeek						= Calendar.MONDAY;
	
	private static ArrayList<String> methods = new ArrayList<String>();
	
	static 
	{
			methods.add(METHOD_WEEK_MONDAY);
			methods.add(METHOD_WEEK_SUNDAY);
			methods.add(METHOD_MONTH_LAST_DAY);
			methods.add(METHOD_MONTH_FIRST_DAY);

	};
	
	public static boolean containsMethod(String method)
	{
		return methods.contains(method);
	}
	
	public static void setFirstDayOfWeek(int firstDay)
	{
		firstDayOfWeek = firstDay;
	}
	
	public static int getFirstDayOfWeek()
	{
		return firstDayOfWeek;
	}
	
	public static String getWeekMonday(Integer offset, String dateFormat)
	{
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		
		Calendar calendar = Calendar.getInstance();
		calendar.setFirstDayOfWeek(firstDayOfWeek);
		calendar.set(Calendar.WEEK_OF_YEAR,calendar.get(Calendar.WEEK_OF_YEAR) + offset);
		calendar.set(Calendar.DAY_OF_WEEK,Calendar.MONDAY);
	    return sdf.format(calendar.getTime());

	}
	
	public static String getWeekSunday(Integer offset, String dateFormat )
	{
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		
		Calendar calendar = Calendar.getInstance();
		calendar.setFirstDayOfWeek(firstDayOfWeek);
		calendar.set(Calendar.WEEK_OF_YEAR,calendar.get(Calendar.WEEK_OF_YEAR) + offset);
		calendar.set(Calendar.DAY_OF_WEEK,Calendar.SUNDAY);
	    return sdf.format(calendar.getTime());

	}
	
	public static String getMonthLastDay(Integer offset, String dateFormat )
	{
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		
		Calendar calendar = Calendar.getInstance();

		calendar.set(Calendar.DAY_OF_MONTH,1);
		calendar.set(Calendar.MONTH,calendar.get(Calendar.MONTH) + offset);
		calendar.set(Calendar.DAY_OF_MONTH,calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
	    return sdf.format(calendar.getTime());

	}
	
	public static String getMonthFirstDay(Integer offset, String dateFormat )
	{
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		
		Calendar calendar = Calendar.getInstance();

		calendar.set(Calendar.DAY_OF_MONTH,1);
		calendar.set(Calendar.MONTH,calendar.get(Calendar.MONTH) + offset);
		calendar.set(Calendar.DAY_OF_MONTH,calendar.getActualMinimum(Calendar.DAY_OF_MONTH));
	    return sdf.format(calendar.getTime());

	}
}
