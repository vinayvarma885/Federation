package com.gathi.mdm.federation;

import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;

public class QueryUtils {
	
	final static Logger LOG = Logger.getLogger(Preprocessor.class);
	
	public static String imputeQuery(String placeholderRegEx, String value, String query){		
		return query.replaceAll(placeholderRegEx, value);
	}
	
	public static String imputeDynamicConditions(String[] values, String query){
		int numConditions = values.length;
		for(int cond = 1; cond <= numConditions; cond++){
			String placeHolderPattern = String.format(FederationConfigs.DYNAMIC_CONDITION_TEMPLATE, cond);
			query = imputeQuery(placeHolderPattern, values[cond-1], query);
		}
		return query;
	}
	
	public static String imputeDynamicConditions(JSONArray values, String query){
		int numConditions = values.length();
		int cond = 1;
		try{			
			for(cond = 1; cond <= numConditions; cond++){
				String placeHolderPattern = String.format(FederationConfigs.DYNAMIC_CONDITION_TEMPLATE, cond);
				query = imputeQuery(placeHolderPattern, values.getString(cond - 1), query);
			}
		}catch(JSONException e){
			LOG.error("No value found for dynamic-condition " + cond);
			System.exit(4);
		}
		return query;
	}
	
	public static String unCommentQuery(String query){
		Pattern multiLineCommentPattern = Pattern.compile("/\\*.*?\\*/", Pattern.DOTALL);
		Pattern singleLineCommentPattern = Pattern.compile("--.*");
		query = multiLineCommentPattern.matcher(query).replaceAll(" ");
		String cleanedQuery = "";
		for( String s: query.split("\n")) {
			s = s.replace("\n", " ");
			s = s.replace("\r", " ");
			if(s.contains("--")){
				if(StringUtils.countMatches(s.substring(0, s.indexOf("--")), "'") % 2 == 0) {
					s = singleLineCommentPattern.matcher(s).replaceAll(" ");
				}
			}
			cleanedQuery = cleanedQuery + s + "\n";
		}
		cleanedQuery = cleanedQuery.replaceAll("[\n\r\t ]+", " ");
		return cleanedQuery;
	}
	
	public static boolean validateQuerySafety(String query){
		boolean isQuerySafe = true;
		for(String operation : FederationConfigs.UNSAFE_OPERATIONS){
			if(query.toLowerCase().contains(operation.toLowerCase())){
				isQuerySafe = false; 
				break;
			}
		}
		return isQuerySafe;
	}
	
	public static int getDynamicConditionsCount(String sql){
		return StringUtils.countMatches(sql, FederationConfigs.DYNAMIC_CONDITION_TEMPLATE);
	}
	
}
