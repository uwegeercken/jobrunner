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

public class VariableReplacer
{
	public static String getVariableName(String value)throws Exception
    {
		String variableValue = stripVariableMarkings(value);
		if(value!=null)
		{
			return variableValue.split(":")[0];
		}
		return null;
    }
	
	private static String stripVariableMarkings(String value)
	{
		String strippedValue = null;
		if(isVariable(value))
        {
        	if(value.substring(1,2).equals("{") && value.substring(value.length()-1).equals("}"))
	        {
        		strippedValue = value.substring(2,value.length()-1);
	        }
	        else
	        {
	        	strippedValue = value.substring(1);
	        }
        }
		else if(value!=null && !value.trim().equals("")) 
        {
        	strippedValue = value;
        }
        
		if(strippedValue!=null)
		{
			return strippedValue;
		}
		else
		{
			return null;
		}
	}
	
	public static boolean isVariable(String value)
	{
		if(value!=null && !value.trim().equals("") && value.startsWith("$"))
        {
			return true;
        }
		else
		{
			return false;
		}
	}
	
	public static int getOffset(String value)
	{
		String variableValue = stripVariableMarkings(value);
		int offsetValue = 0;
		if(value!=null)
		{
			String parts[] = variableValue.split(":");
			if(parts.length==2)
			{
				try
				{
					offsetValue = Integer.parseInt(parts[1]);
				}
				catch(Exception ex)
				{
					offsetValue = 0;
				}
			}
		}
		return offsetValue;
	}
}
