package com.gathi.mdm.federation;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.jute.compiler.generated.ParseException;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.gathi.mdm.federation.Preprocessor;

public class Federation {
	final static Logger LOG = Logger.getLogger(Federation.class);
//	static Logger log = LogManager.getLogger(Federation.class);
	
	public static void main(String[] args){
		String appName = null;
		String configFilePath = null;
		String env = null;
		String LOG_PATH = null;
		Options options = new Options();
		options.addOption("APP_NAME", true, "APP_NAME");
		options.addOption("CONFIG_FILE_PATH", true, "CONFIG_FILE");
		options.addOption("LOG_PATH", true, "LOG_PATH");
		options.addOption("ENV", true, "ENV");
		CommandLineParser parser = new PosixParser();
		try{
			CommandLine line = parser.parse(options, args);
			if(line!=null){
				if(line.hasOption("APP_NAME")){
					appName = line.getOptionValue("APP_NAME");
				}else{
					throw new ParseException("Arg: APP_NAME is missing");
				}
				if(line.hasOption("CONFIG_FILE_PATH")){
					configFilePath = line.getOptionValue("CONFIG_FILE_PATH");
				}else{
					throw new ParseException("Arg: CONFIG_FILE_PATH is missing");
				}
				if(line.hasOption("ENV")){
					env = line.getOptionValue("ENV");
				}else{
					throw new ParseException("Arg: ENV is missing");
				}
				if(line.hasOption("LOG_PATH")){
					LOG_PATH = line.getOptionValue("LOG_PATH");
				}else{
					throw new ParseException("Arg: LOG_PATH is missing");
				}
				/*if(line.hasOption("DDLGENERATOR")){
					LOG_PATH = line.getOptionValue("DDLGENERATOR");
				}else{
					throw new ParseException("Arg: LOG_PATH is missing");
				}*/
			}else{
				throw new ParseException("Missing Arguments, See Usage.");
			}
		}catch(Exception e){
			e.printStackTrace(System.out);
			System.out.println("spark-submit --class com.gati.Federation --master local[2] "
					+ "--jars mysql-connector-java-5.1.6.jar,postgresql-42.2.2.jre7.jar,utils-0.1.jar "
					+ "TestSpark-0.1.jar -APP_NAME testJobName -CONFIG_FILE_PATH /home/hduser/federation/testFederation.json");
		}
		
		// set environment
		// TODO : get environment from $ENV
		System.setProperty("ENV", env);
		System.setProperty("LOG_PATH", LOG_PATH);
		FederationLogger.configureLogger(LOG_PATH);
		
		SparkSession spark = SparkUtils.getOrCreateSparkSession(appName);
//		SQLContext sqlContext = SparkUtils.getSQLContext(ctx) ;
		
		JSONObject federationJSONConfig = null;
		try{
			federationJSONConfig = new JSONObject(SparkUtils.getFileContent(configFilePath));
			//System.out.println("json"+federationJSONConfig.toString());
		}catch(IOException e){
			LOG.error("Failed to read config file", e);
			System.exit(2);
		}catch(JSONException e){
			LOG.error("Incorrectly Specified JSON config", e);
			System.exit(4);
		}
		
		try{
			federate(spark, federationJSONConfig);
		}catch(Exception e){
			LOG.error("Federation failed with ", e);
			spark.stop();
			System.exit(1);
		}
		LOG.info("Federation successfuly completed");
		spark.stop();
		System.exit(0);
	}
	
	private static boolean federate(SparkSession spark, 
			JSONObject federationJSONConfig) throws Exception{
		
		// step 1: pre-processing : validation, safety check, dynamic-imputation, alias generation
		String application_id = spark.sparkContext().applicationId();
		federationJSONConfig = Preprocessor.process(federationJSONConfig, application_id);
		//System.out.println("jsoninput");
		// step 2: fetch data as data-frame
		JSONArray inputJSONs = federationJSONConfig.getJSONArray("inputs");
		//System.out.println("jsoninput2"+inputJSONs.toString());
		
		Dataset<Row>[] inputDFs = SparkUtils.generateInputDFs(inputJSONs, spark);
		String filetype="";
		String alias="";
		String sql="";
		// step 3: register dataframes as temp tables
		for(int i = 0; i < inputDFs.length; i++){
			//System.out.println("alias"+inputJSONs.getJSONObject(i).getString("alias"));
			//System.out.println("data"+inputJSONs.getJSONObject(i).getString("datasource"));	

			inputDFs[i].createOrReplaceTempView(inputJSONs.getJSONObject(i).getString("alias"));
			if(inputJSONs.getJSONObject(i).getString("datasource").equals("S3")) {
				filetype="S3";
				System.out.println("filetype"+filetype);
				alias=inputJSONs.getJSONObject(i).getString("alias");
				//sql=inputJSONs.getJSONObject(i).getString("sql");
			}
			
/*			inputDFs[i].show();
			inputDFs[i].schema().printTreeString();*/
		}
		/*
		for(int i=0;i<inputJSONs.length();i++) {
			JSONObject rec = inputJSONs.getJSONObject(i);
		}*/
		// step 4: execute sql
       //	System.out.println("sql:"+federationJSONConfig.getString("executable-sql"));
       	//System.out.println("sql1:"+federationJSONConfig.getString("sql"));
		System.out.println("filetype"+filetype);
		Dataset<Row> outputDF ;
		System.out.println("We are near executable sql print stmt");
		System.out.println("HELLOOOOOOOOOO "+federationJSONConfig.getString("executable-sql"));
		/*if(filetype.equals("S3")) {
			 outputDF = spark.sql("select * from "+alias);
		}
	
		
		else {*/
			
		//if(inputJSONs.getJSONObject("config").getString("datasource")) {
		 outputDF = spark.sql(federationJSONConfig.getString("executable-sql"));
		//}
		//outputDF.show();
		//Dataset<Row> outputDF = spark.sql(federationJSONConfig.getString("sql"));
		//System.out.println("&&&&&result&&&");
		//outputDF.show(10);
		// step 5: write data-frame to outputs
		JSONArray outputJSONs = federationJSONConfig.getJSONArray("outputs");
		SparkUtils.writeDFtoTarget(outputJSONs, outputDF, spark);
		
		//step 6: write ddl
		
		return true;
	}
}
