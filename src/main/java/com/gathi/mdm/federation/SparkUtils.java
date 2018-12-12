package com.gathi.mdm.federation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.JSONArray;
import org.json.JSONObject;

public class SparkUtils {
	
	static Logger log = Logger.getLogger(SparkUtils.class);
	
	public static SparkSession getOrCreateSparkSession(String appName) {
		SparkSession spark = SparkSession.builder().appName(appName).enableHiveSupport().getOrCreate();
		return spark;
	}
	
	@Deprecated
	public static SQLContext getSQLContext(JavaSparkContext ctx) {
		final SQLContext sqlContext = new SQLContext(ctx);
		return sqlContext;
	}

	@Deprecated
	public static JavaSparkContext getJavaContext(String appName) {
		final SparkConf sparkConf = new SparkConf().setAppName(appName);/*.setMaster("local[*]")
				.set("spark.scheduler.mode", "FAIR")
				.set("spark.mongodb.input.uri", "mongodb://staging:StgUsr05@52.203.109.83:27017/")
                .set("spark.mongodb.input.database", "staging")
                .set("spark.mongodb.input.collection","test2")
				.set("spark.mongodb.output.uri", "mongodb://staging:StgUsr05@52.203.109.83:27017/")
                .set("spark.mongodb.output.database", "staging")
                .set("spark.mongodb.output.collection","test2");*/
		final JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		
		/*Configuration hadoopConf=ctx.hadoopConfiguration();
		hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");
		hadoopConf.set("fs.s3.awsAccessKeyId","AKIAIGJVLUPRIHN6EBYA");
		hadoopConf.set("fs.s3.awsSecretAccessKey","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");*/
		return ctx;
	}

	public static String getFileContent(String jsonFile) throws IOException{
//		System.out.println("jsonFile: " + jsonFile);
		BufferedReader br = new BufferedReader(new FileReader(jsonFile));
		String json = ""; String s;
		while((s=br.readLine()) != null){
			json += s;
		}
		br.close();
	//System.out.println("Input JSON String:" + json);
		//return new JSONObject(json);
		return json;
	}

	public static Dataset<Row>[] generateInputDFs(JSONArray inputJSONs, SparkSession spark)
			throws Exception {
		Dataset<Row>[] inputDFs = new Dataset[inputJSONs.length()];	
		for (int i = 0; i < inputJSONs.length(); i++) {
			JSONObject inputJSON = inputJSONs.getJSONObject(i);
			String type = inputJSON.getJSONObject("config").getString("type");
			System.out.println("type"+type);
			if (type.equalsIgnoreCase("postgres"))
				inputDFs[i] = DataFrameUtils.createDataFrameFromPostgresDB(inputJSON, spark);
			else if(type.equalsIgnoreCase("mysql"))
				inputDFs[i] = DataFrameUtils.createDataFrameFromDB(inputJSON, spark);
			else if (type.equalsIgnoreCase("hdfs"))
				inputDFs[i] = DataFrameUtils.createDataFrameFromHDFS(inputJSON, spark);
			else if (type.equalsIgnoreCase("local"))
				inputDFs[i] = DataFrameUtils.createDataFrameFromLocal(inputJSON, spark);
			else if (type.equalsIgnoreCase("s3"))
				inputDFs[i] = DataFrameUtils.createDataFrameFromS3(inputJSON, spark);             // hdfs local s3 all are same???
			/*else if (type.equalsIgnoreCase("parquet"))
				inputDFs[i] = DataFrameUtils.createDataFrameFromParquet(inputJSON, sqlContext);
			else if (type.equalsIgnoreCase("json"))
				inputDFs[i] = DataFrameUtils.createDataFrameFromJSON(inputJSON, sqlContext);*/
			else if (type.equalsIgnoreCase("mongo"))
				inputDFs[i] = DataFrameUtils.createDataFrameFromMongo(inputJSON, spark);
			else if (type.equalsIgnoreCase("hive"))
				inputDFs[i] = DataFrameUtils.createDataFrameFromHive(inputJSON, spark);
			else if(type.equalsIgnoreCase("mssql"))
				inputDFs[i] = DataFrameUtils.createDataFrameFromMSSQL(inputJSON,spark);
			else if(type.equalsIgnoreCase("oracle"))
				inputDFs[i] = DataFrameUtils.createDataFrameFromOracle(inputJSON,spark);
			else if(type.equalsIgnoreCase("ibmdb2"))
				inputDFs[i] = DataFrameUtils.createDataFrameFromOracle(inputJSON,spark);
			else if(type.equalsIgnoreCase("FILES"))
				inputDFs[i] = DataFrameUtils.createDataFrameFromFiles(inputJSON,spark);
			
			else
				throw new Exception("unsupported type is supported in the type:" + type);
		}
		return inputDFs;
	}

	public static void writeDFtoTarget(JSONArray outputJSONs, Dataset<Row> dataFrame, SparkSession spark) throws Exception{
		
		for(int i = 0; i < outputJSONs.length(); i++){
			JSONObject outputJSON = outputJSONs.getJSONObject(i);
			String type = outputJSON.getJSONObject("config").getString("type");
			if(type.equalsIgnoreCase("mysql"))
				DataFrameUtils.writeDataFrameToDB(dataFrame, outputJSON);
			else if(type.equalsIgnoreCase("postgres"))
				DataFrameUtils.writeDataFrameToPostgresDB(dataFrame, outputJSON);
			else if(type.equalsIgnoreCase("hdfs"))
				DataFrameUtils.writeDataFrameToHDFS(dataFrame, outputJSON, spark);
			else if(type.equalsIgnoreCase("local"))
				DataFrameUtils.writeDataFrameToLocal(dataFrame, outputJSON);
			else if(type.equalsIgnoreCase("s3"))
				DataFrameUtils.writeDataFrameToS3(dataFrame, outputJSON, spark);
			/*else if(type.equalsIgnoreCase("parquet"))
				DataFrameUtils.writeDataFrameToParquet(dataFrame, outputJSON,  sqlContext);
			else if(type.equalsIgnoreCase("json"))
				DataFrameUtils.writeDataFrameToJSON(dataFrame, outputJSON);
			else if(type.equalsIgnoreCase("mongo"))
				DataFrameUtils.writeDataFrameToMongo(dataFrame, outputJSON, ctx);*/
			else
				throw new Exception("unsupported type is supported in the type:" + type);
		}
	}	
	
	// [{"name":"column_1", "datatype":"int", "nullable":}]
	public static StructType convertToSparkSchema(JSONArray schema){
		int numberOfColumns = schema.length();
		StructField[] sparkFields = new StructField[numberOfColumns];
		try{
		for(int i = 0; i < numberOfColumns; i++){
			JSONObject fieldJSON = schema.getJSONObject(i);
			StructField field = new StructField(fieldJSON.getString("name"),
					mapToSparkDataType(fieldJSON.getString("datatype")), 
					fieldJSON.getBoolean("nullable"),
					Metadata.empty());
			sparkFields[i] = field;
		}
		}catch(Exception e){
			log.error("Failed to convert provided schema to spark dataframe schema with : ", e);
			System.exit(1);
		}
		return new StructType(sparkFields);
	}
	
	/*
	 * https://stackoverflow.com/questions/32899701/what-is-the-scala-type-mapping-for-all-spark-sql-datatype
	 */
	public static DataType mapToSparkDataType(String inputDataType){
		String dataType = inputDataType;
		Pattern dataTypePattern = Pattern.compile("([A-Za-z]+)(\\((\\d+),(\\d+)\\))*");
		Matcher matcher = dataTypePattern.matcher(dataType);
		int precision = 0;
		int scale = 0;
		try{
	    if(matcher.find())
	    {
	    	if(matcher.group(2)!=null){
	    		dataType = matcher.group(1);
	    		precision = Integer.parseInt(matcher.group(3));
	    		scale = Integer.parseInt(matcher.group(4));
	    	}
	    }
		} catch(Exception e){
			log.error("Incorrect DataType format. Found " + inputDataType + ". Please correct schema.");
			System.exit(4);
		}
		
		dataType = dataType.toLowerCase(); 
		DataType sparkDataType = null;
		
		if(dataType.equals("byte")){
			sparkDataType = DataTypes.ByteType;
		}
		else if(dataType.equals("short")){
			sparkDataType = DataTypes.ShortType;
		}
		else if(dataType.equals("int") || dataType.equals("integer")){
			sparkDataType = DataTypes.IntegerType;
		}
		else if(dataType.equals("long")){
			sparkDataType = DataTypes.LongType;
		}
		else if(dataType.equals("float")){
			sparkDataType = DataTypes.FloatType;
		}
		else if(dataType.equals("double")){
			sparkDataType = DataTypes.DoubleType;
		}
		else if(dataType.equals("decimal")){
			sparkDataType = DataTypes.createDecimalType(precision, scale);
		}
		else if(dataType.equals("string")){
			sparkDataType = DataTypes.StringType;
		}
		else if(dataType.equals("byte[]")){
			sparkDataType = DataTypes.BinaryType;
		}
		else if(dataType.equals("string") || dataType.equals("varchar") || dataType.equals("char")){
			sparkDataType = DataTypes.StringType;
		}
		else if(dataType.equals("boolean")){
			sparkDataType = DataTypes.BooleanType;
		}
		else if(dataType.equals("timestamp")){
			sparkDataType = DataTypes.TimestampType;
		}
		else if(dataType.equals("date")){
			sparkDataType = DataTypes.DateType;
		}
		else if(dataType.equals("timestamp")){
			sparkDataType = DataTypes.TimestampType;
		}
	/*	else if(dataType.equals("list")){
			sparkDataType = DataTypes.createArrayType(elementType);
		}*/
		else{
			log.error("Unsuppoted/invalid data type " + inputDataType);
			System.exit(4);
		}
		return sparkDataType;
	}
	
}
