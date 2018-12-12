package com.gathi.mdm.federation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;

import java.util.Properties;

import org.json.JSONException;
import org.json.JSONObject;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

public class DataFrameUtils {
	
	final static Logger LOG = Logger.getLogger(DataFrameUtils.class);
	
	public static Dataset<Row> createDataFrameFromDB(JSONObject inputJSON, SparkSession spark) throws Exception{
		Dataset<Row> df = null;
		System.out.println("mysql"+inputJSON.toString());
		JSONObject config = inputJSON.getJSONObject("config");
		String databaseName = inputJSON.getString("database");
		String tableName = inputJSON.getString("table");
		String url = config.getString("jdbc-url");		
		String driver = config.getString("driver");
		String user = inputJSON.getString("user");
		String password = config.getString("password");
		
		//dbtable = databse.table
		String dbtable = String.format("%s.%s", databaseName, tableName);
		HashMap<String, String> sourceMap = new HashMap<String, String>();
		sourceMap.put("url", url);
		sourceMap.put("driver", driver);
		sourceMap.put("user", user);
		sourceMap.put("password", password);
		sourceMap.put("dbtable", dbtable);
	//	sourceMap.put("numPartitions", noOfPartitions);
		//System.out.println("***** sourceMap: " + sourceMap);
		//df = sqlContext.load("jdbc", sourceMap);
		df = spark.read().format("jdbc").options(sourceMap).load();
//		df.show();
		return df;
	}
	public static Dataset<Row> createDataFrameFromPostgresDB(JSONObject inputJSON, SparkSession spark) throws Exception{
		Dataset<Row> df = null;
		System.out.println("mysql"+inputJSON.toString());
		JSONObject config = inputJSON.getJSONObject("config");
		String databaseName = inputJSON.getString("database");
		String schemaName = inputJSON.getString("schema");
		String tableName = inputJSON.getString("table");
		String user = inputJSON.getString("user");
		String url = config.getString("jdbc-url");		
		String driver = config.getString("driver");
		String password = config.getString("password");
		
		//dbtable = schema.table
		String dbtable = String.format("\"%s\".%s", schemaName, tableName);
		if(url.endsWith("/"))
			url = url + databaseName;
		else
			url = url + "/" + databaseName;	
	
		HashMap<String, String> sourceMap = new HashMap<String, String>();
		sourceMap.put("url", url);
		sourceMap.put("driver", driver);
		sourceMap.put("user", user);
		sourceMap.put("password", password);
		sourceMap.put("dbtable", dbtable);
	//	sourceMap.put("numPartitions", noOfPartitions);
		//System.out.println("***** sourceMap: " + sourceMap);
		//df = sqlContext.load("jdbc", sourceMap);
		df = spark.read().format("jdbc").options(sourceMap).load();
//		df.show();
		return df;
	}
	
	public static Dataset<Row> createDataFrameFromHDFS(JSONObject inputJSON, SparkSession spark) throws Exception{
		Dataset<Row> df = null;
		String fileType = inputJSON.getString("file-type");
		if(fileType.equalsIgnoreCase("parquet"))
			df = createDataFrameFromParquet(inputJSON, spark);
		else if(fileType.equalsIgnoreCase("json")) 
			df = createDataFrameFromJSON(inputJSON, spark);
		else if(fileType.equalsIgnoreCase("csv")) 
			df = createDataFrameFromCSV(inputJSON, spark);
		else{ 
				LOG.error("Unsupported file type in input"); 
				System.exit(4);
		}	
		return df;
	}
	public static Dataset<Row> createDataFrameFromLocal(JSONObject inputJSON, SparkSession spark) throws Exception{
		Dataset<Row> df = null;
		String fileType = inputJSON.getString("file-type");
		if(fileType.equalsIgnoreCase("parquet"))
			df = createDataFrameFromParquet(inputJSON, spark);
		else if(fileType.equalsIgnoreCase("json")) 
			df = createDataFrameFromJSON(inputJSON, spark);
		else if(fileType.equalsIgnoreCase("csv")) 
			df = createDataFrameFromCSV(inputJSON, spark);
		else{ 
				LOG.error("Unsupported file type in input"); 
				System.exit(4);
		}	
		return df;
	}
	
	public static Dataset<Row> createDataFrameFromS3(JSONObject inputJSON, SparkSession spark) throws Exception{
		Dataset<Row> df = null;
		String fileType = inputJSON.getString("file-type");
//		System.out.println(inputJSON.toString());
		spark.sparkContext().hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");
//		System.out.println(">> " + inputJSON.getJSONObject("config").getString("fs.s3.awsAccessKeyId"));
//		System.out.println(">> " + inputJSON.getJSONObject("config").getString("password"));
		spark.sparkContext().hadoopConfiguration().set("fs.s3.awsAccessKeyId", inputJSON.getJSONObject("config").getString("fs.s3.awsAccessKeyId"));
		spark.sparkContext().hadoopConfiguration().set("fs.s3.awsSecretAccessKey", inputJSON.getJSONObject("config").getString("password"));
		
		
		
		if(fileType.equalsIgnoreCase("parquet"))
			df = createDataFrameFromParquet(inputJSON, spark);
		else if(fileType.equalsIgnoreCase("json")) 
			df = createDataFrameFromJSON(inputJSON, spark);
		else if(fileType.equalsIgnoreCase("csv"))
			df = createDataFrameFromCSV(inputJSON, spark);
		else{ 
				LOG.error("Unsupported file type in input"); 
				System.exit(4);
		}	

		return df;
	}
	
	public static Dataset<Row> createDataFrameFromParquet(JSONObject inputJSON, SparkSession spark) throws Exception{
		Dataset<Row> df = null;
/*		String url = inputJSON.getString("url");
		String path = inputJSON.getString("table");
		df = sqlContext.read().parquet(url + "/" + path);*/
		String path = inputJSON.getString("file-path");
		//System.
		df = spark.read().parquet(path);
		return df;
	}
	
	public static Dataset<Row> createDataFrameFromCSV(JSONObject inputJSON, SparkSession spark) throws Exception{
		Dataset<Row> df = null;
		StructType schema = null;
		
		boolean inferSchema = inputJSON.getString("inferSchema").equalsIgnoreCase("true") ? true : false;
		String path = inputJSON.getString("file-path");
		String header = inputJSON.getString("header");
		String delimiter = inputJSON.getString("delimiter");
		String quote = inputJSON.getString("quote");
		String escape = inputJSON.getString("escape");
	//	String parserLib = inputJSON.getString("parserLib");
	//	String mode = inputJSON.getString("mode");
	//	String charset = inputJSON.getString("charset");
	//	String comment = inputJSON.getString("comment");
		String nullValue = inputJSON.getString("nullValue");
		String dateFormat = inputJSON.getString("dateFormat");
		
		if(!inferSchema){
			schema  = SparkUtils.convertToSparkSchema(inputJSON.getJSONArray("schema"));
		
			df = spark.read().format("com.databricks.spark.csv").schema(schema).
					option("header",header).
					option("delimiter",delimiter).
					option("quote",quote).
					option("escape",escape).
			//		option("parserLib",parserLib).
			//		option("mode",mode).
			//		option("charset",charset).
			//		option("comment",comment).
					option("nullValue",nullValue).
					option("dateFormat",dateFormat).
					load(path);
		}
		else{
			df = spark.read().format("com.databricks.spark.csv").
					option("inferSchema","false").
					option("header",header).
					option("delimiter",delimiter).
					option("quote",quote).
					option("escape",escape).
			//		option("parserLib",parserLib).
			//		option("mode",mode).
			//		option("charset",charset).
			//		option("comment",comment).
					option("nullValue",nullValue).
					option("dateFormat",dateFormat).
					load(path);
		}
//		df.show();
		return df;
	}
	
	public static Dataset<Row> createDataFrameFromJSON(JSONObject inputJSON, SparkSession spark) throws Exception{
		Dataset<Row> df = null;
		StructType schema = null;
/*		String url = inputJSON.getString("url");
		String path = inputJSON.getString("table");
		System.out.println("In createDataFrameFromJSON: " + url + "/"+ path);
		df = sqlContext.read().json(url + "/" + path);
		df.show();*/
		String path = inputJSON.getString("file-path");
		boolean inferSchema = inputJSON.getString("inferSchema").equalsIgnoreCase("true") ? true : false;
		if(!inferSchema){
			schema  = SparkUtils.convertToSparkSchema(inputJSON.getJSONArray("schema"));
			df = spark.read().schema(schema).json(path);
		}
		else{
			df = spark.read().json(path);
		}
		return df;
	}
	
	public static Dataset<Row> createDataFrameFromMongo(JSONObject inputJSON, SparkSession spark) throws Exception{
		Dataset<Row> df = null;
		Map<String, String> readOverRides = new HashMap<String, String>();
		String url = inputJSON.getString("url");
		df = spark.read().format(inputJSON.getString("driver"))
				.option("spark.mongodb.input.uri", inputJSON.getString("uri") + "/" + inputJSON.getString("user")).load();
		return df;
	}
	
/*	public static boolean writeDataFrameToParquet(DataFrame df, JSONObject inputJSON, SQLContext sqlContext) throws Exception {
		String url = inputJSON.getString("url");
		String path = inputJSON.getString("table");
		boolean append = inputJSON.getBoolean("append");
		
		df.registerTempTable("TEMP_TABLE");
		String sql = createSelectStatementForDF(df, "TEMP_TABLE");
		System.out.println("sql:" + sql);
		DataFrame df2 = sqlContext.sql(sql);
		if(append){
			df2.write().mode(SaveMode.Append).parquet(url + "/" + path);

			System.out.println("*******************In writeDataFrameToParquet: Append: before writing to S3");
			df2.write().format("json").mode(SaveMode.Append).save("s3://gathi-bigdata/RAW_ZONE/" + path);
			.option("fs.s3.awsAccessKeyId","AKIAIGJVLUPRIHN6EBYA")
			  .option("fs.s3.awsSecretAccessKey","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")
		}
		else
			df2.write().mode(SaveMode.Overwrite).parquet(url + "/" + path);
		
			System.out.println("*******************In writeDataFrameToParquet: Overwrite: before writing to S3");
			df2.write().format("json").mode(SaveMode.Overwrite).save("s3://gathi-bigdata/RAW_ZONE/" + path);
		return true;
	}*/
	public static boolean writeDataFrameToHDFS(Dataset<Row> dataframe, JSONObject outputJSON, SparkSession spark) throws Exception{
		boolean success = false;
		String fileType = outputJSON.getString("file-type");
		if(fileType.equalsIgnoreCase("parquet"))
			success = writeDataFrameToParquet(dataframe, outputJSON);
		else if(fileType.equalsIgnoreCase("json"))
			success =  writeDataFrameToJSON(dataframe, outputJSON);
		else if(fileType.equalsIgnoreCase("csv"))
			success =  writeDataFrameToCSV(dataframe, outputJSON);
		else{
				LOG.error("Unsupported file type in output"); 
				System.exit(4);
		}	
		return success;
	}
	
	public static boolean writeDataFrameToLocal(Dataset<Row> dataframe, JSONObject outputJSON) throws Exception{
		boolean success = false;
		String fileType = outputJSON.getString("file-type");
		if(fileType.equalsIgnoreCase("parquet"))
			success = writeDataFrameToParquet(dataframe, outputJSON);
		else if(fileType.equalsIgnoreCase("json"))
			success =  writeDataFrameToJSON(dataframe, outputJSON);
		else if(fileType.equalsIgnoreCase("csv"))
			success =  writeDataFrameToCSV(dataframe, outputJSON);
		else{
				LOG.error("Unsupported file type in output"); 
				System.exit(4);
		}	
		return success;
	}
	
	public static boolean writeDataFrameToS3(Dataset<Row> dataframe, JSONObject outputJSON, SparkSession spark) throws Exception{
		boolean success = false;
		String fileType = outputJSON.getString("file-type");
		spark.sparkContext().hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");
		spark.sparkContext().hadoopConfiguration().set("fs.s3.awsAccessKeyId", outputJSON.getJSONObject("config").getString("fs.s3.awsAccessKeyId"));
		spark.sparkContext().hadoopConfiguration().set("fs.s3.awsSecretAccessKey", outputJSON.getJSONObject("config").getString("password"));
		if(fileType.equalsIgnoreCase("parquet"))
			success = writeDataFrameToParquet(dataframe, outputJSON);
		else if(fileType.equalsIgnoreCase("json"))
			success =  writeDataFrameToJSON(dataframe, outputJSON);
		else if(fileType.equalsIgnoreCase("csv"))
			success =  writeDataFrameToCSV(dataframe, outputJSON);
		else{
				LOG.error("Unsupported file type in output"); 
				System.exit(4);
		}	
		return success;
	}
	
	public static boolean writeDataFrameToParquet(Dataset<Row> df, JSONObject outputJSON) throws Exception {
		
		String path = outputJSON.getString("file-path");
		boolean append = outputJSON.getBoolean("append");
	
		if(outputJSON.has("output-partitions-count")){
			df = df.repartition(outputJSON.getInt("output-partitions-count"));
		}
		if(append)
			df.write().mode(SaveMode.Append).parquet(path);
		else
			df.write().mode(SaveMode.Overwrite).parquet(path);
		return true;
	}
	
	
	public static boolean writeDataFrameToDatabase(Dataset<Row> df, JSONObject outputJSON)throws Exception {
		Properties props = new Properties();
		String url  	= outputJSON.getString("url");
		String driver 	= outputJSON.getString("driver");
		String user 	= outputJSON.getString("user");
		String password = outputJSON.getString("password");
	//	String noOfPartitions = inputJSON.getString("partitions");
		String table = outputJSON.getString("table");
		
		props.setProperty("user", user);
		props.setProperty("password", password);
		props.setProperty("driver", driver);
	//	props.setProperty("numPartitions", noOfPartitions);

		boolean append = outputJSON.getBoolean("append");
		if(append)
			df.write().mode(SaveMode.Append).jdbc(url, table, props);
		else
			df.write().mode(SaveMode.Overwrite).jdbc(url, table, props);
		return true;
	}
	public static Dataset<Row> createDataFrameFromHive(JSONObject inputJSON, SparkSession spark) throws Exception{
		Dataset<Row> df = null;
		JSONObject config = inputJSON.getJSONObject("config");
		String databaseName = inputJSON.getString("database");
		//String schemaName = inputJSON.getString("schema");
		String tableName = inputJSON.getString("table");
		System.out.println("databasename"+databaseName+"tablename"+tableName);
		df=spark.sql("select * from "+databaseName+"."+tableName);
		return df;

		
	}
	public static boolean writeDataFrameToDB(Dataset<Row> df, JSONObject outputJSON)throws Exception {
		
		JSONObject config = outputJSON.getJSONObject("config");
		String url  	= config.getString("jdbc-url");
		String driver 	= config.getString("driver");
		String user 	= outputJSON.getString("user");
		String password = config.getString("password");
	//	String noOfPartitions = inputJSON.getString("partitions");
		String database = outputJSON.getString("database");
		String table = outputJSON.getString("table");
		boolean append = outputJSON.getBoolean("append");
		
		String dbtable = database + "." + table;
		if(url.endsWith("/"))
			url = url + database;
		else
			url = url + "/" + database;	
	
		HashMap<String, String> sourceMap = new HashMap<String, String>();
		sourceMap.put("url", url);
		sourceMap.put("driver", driver);
		sourceMap.put("user", user);
		sourceMap.put("password", password);
		sourceMap.put("dbtable", dbtable);
	//	sourceMap.put("numPartitions", noOfPartitions);
		//System.out.println("***** sourceMap: " + sourceMap);
		//df = sqlContext.load("jdbc", sourceMap);
		Properties props = new Properties();
		props.put("driver", driver);
		props.put("user", user);
		props.put("password", password);
		props.put("dbtable", dbtable);
		props.put("url", url);
		if(append){
			df.write().mode(SaveMode.Append).jdbc(url, dbtable, props);
			}
		else{
			df.write().mode(SaveMode.Overwrite).jdbc(url, dbtable, props);
			}
		return true;
	}
	
	public static boolean writeDataFrameToPostgresDB(Dataset<Row> df, JSONObject outputJSON)throws Exception {
//		System.out.println(outputJSON.toString());
		JSONObject config = outputJSON.getJSONObject("config");
		String url  	= config.getString("jdbc-url");
		String driver 	= config.getString("driver");
		String user 	= outputJSON.getString("user");
		String password = config.getString("password");
	//	String noOfPartitions = inputJSON.getString("partitions");
		String databaseName = outputJSON.getString("database");
		String table = outputJSON.getString("table");
		String schema = outputJSON.getString("schema");
		boolean append = outputJSON.getBoolean("append");
		String dbtable = String.format("\"%s\".%s", schema, table);
		//String dbtable = schema + "." + table;
		if(url.endsWith("/"))
			url = url + databaseName;
		else
			url = url + "/" + databaseName;	
		
		Properties props = new Properties();
		props.put("driver", driver);
		props.put("user", user);
		props.put("password", password);
		props.put("dbtable", dbtable);
		props.put("url", url);
		props.put("user", user);
		if(append){
			df.write().mode(SaveMode.Append).jdbc(url, dbtable, props);
			}
		else{
			df.write().mode(SaveMode.Overwrite).jdbc(url, dbtable, props);
			}
		return true;
	}
	public static boolean executeSQL(String driver, String url, String id, String password, String sql) throws Exception{
		Class.forName(driver);
		Connection conn = DriverManager.getConnection(url, id, password);
		conn.createStatement().execute(sql);
		conn.close();
		return true;
	}
	
	public static boolean writeDataFrameToCSV(Dataset<Row> df, JSONObject outputJSON) throws Exception{
		
		String path = outputJSON.getString("file-path");
		String header = outputJSON.getString("header");
		String delimiter = outputJSON.getString("delimiter");
		String quote = outputJSON.getString("quote");
		String escape = outputJSON.getString("escape");
		String nullValue = outputJSON.getString("nullValue");
		String dateFormat = outputJSON.getString("dateFormat");
		String codec = outputJSON.getString("codec");
		
		if(outputJSON.has("output-partitions-count")){
			df = df.repartition(outputJSON.getInt("output-partitions-count"));
		}
//		df.show();
		df.write().format("com.databricks.spark.csv").
					option("inferSchema", "false").
					option("header", header).
					option("delimiter", delimiter).
					option("quote", quote).
					option("escape", escape).
					option("nullValue", nullValue).
					option("dateFormat", dateFormat).
					save(path);
		
		return true;
	}
	
	
	public static String createSelectStatementForDF(Dataset<Row> df, String table) throws Exception{
		StructType st = df.schema();
		StructField[] fields = st.fields();
		String sql = "(select ";
		for(int i=0;i<fields.length;i++) {
			String name = fields[i].name();
			sql += name + " " + name.toLowerCase() + ",";
		}
		sql = sql.substring(0, sql.length()-1);
		//sql += " from table) Temp_table";
		sql += " from TEMP_TABLE)";
		return sql;
	}
	
	public static String createTableStatement(Dataset<Row> df, String table) throws Exception{
		String sql = "Create Table " + table + " ( ";
		StructType st = df.schema();
		StructField[] fields = st.fields();
		for(int i=0;i<fields.length;i++) {
			String name = fields[i].name();
			String type = fields[i].dataType().typeName();
			if(type.equalsIgnoreCase("text") || type.equalsIgnoreCase("string") )
				type = " VARCHAR(200) ";
			else if (type.equalsIgnoreCase("double") )
				type = " DECIMAL ";
			else if (type.equalsIgnoreCase("long") )
				type = " number(20)";
			sql += name + " " + type + ",";
		}
		sql = sql.substring(0, sql.length()-1);
		return sql;
	}


	public static boolean writeDataFrameToJSON(Dataset<Row> df, JSONObject outputJSON) throws Exception {
		String path = outputJSON.getString("file-path");
		boolean append = outputJSON.getBoolean("append");
		if(outputJSON.has("output-partitions-count")){
			df = df.repartition(outputJSON.getInt("output-partitions-count"));
		}
		df.show();
		if(append)
			df.write().mode(SaveMode.Append).json(path);
		else
			
			//df.write().mode(SaveMode.Overwrite).json(path);
		df.na().fill("null").write().mode(SaveMode.Overwrite).format("json").json(path);
		return true;
	}
	public static Dataset<Row> createDataFrameFromMSSQL(JSONObject inputJSON, SparkSession spark) throws Exception{
		Dataset<Row> df = null;
		JSONObject config = inputJSON.getJSONObject("config");
		String databaseName = inputJSON.getString("database");
		String schemaName = inputJSON.getString("schema");
		String tableName = inputJSON.getString("table");
		String user = inputJSON.getString("user");
		String url = config.getString("jdbc-url");		
		String driver = config.getString("driver");
		String password = config.getString("password");
		
		//dbtable = schema.table
		String dbtable = String.format("\"%s\".%s", schemaName, tableName);
		//System.out.println("databasename:"+databaseName+""+"tablename"+tableName+""+"url"+url+"dbtable:"+dbtable);
		if(url.endsWith("/"))
			url = url + databaseName;
		else
			url = url + ";" +"databaseName="+ databaseName;	
		//System.out.println("databasename:"+databaseName+""+"tablename"+tableName+""+"url"+url);
		HashMap<String, String> sourceMap = new HashMap<String, String>();
		sourceMap.put("url", url);
		sourceMap.put("driver", driver);
		sourceMap.put("user", user);
		sourceMap.put("password", password);
		sourceMap.put("dbtable", dbtable);
	//	sourceMap.put("numPartitions", noOfPartitions);
		//System.out.println("***** sourceMap: " + sourceMap);
		//df = sqlContext.load("jdbc", sourceMap);
		df = spark.read().format("jdbc").options(sourceMap).load();
//		df.show();
		return df;
	}
	public static Dataset<Row> createDataFrameFromOracle(JSONObject inputJSON, SparkSession spark) throws Exception{
		Dataset<Row> df = null;
		JSONObject config = inputJSON.getJSONObject("config");
		String databaseName = inputJSON.getString("database");
		String schemaName = inputJSON.getString("schema");
		String tableName = inputJSON.getString("table");
		String user = inputJSON.getString("user");
		String url = config.getString("jdbc-url");		
		String driver = config.getString("driver");
		String password = config.getString("password");
		
		//dbtable = schema.table
		String dbtable = String.format("\"%s\".%s", schemaName, tableName);
		//System.out.println("databasename:"+databaseName+""+"tablename"+tableName+""+"url"+url+"dbtable:"+dbtable);
		/*if(url.endsWith("/"))
			url = url + databaseName;
		else*/
			url = url;
		System.out.println("url"+url);	
			
		//System.out.println("databasename:"+databaseName+""+"tablename"+tableName+""+"url"+url);
		HashMap<String, String> sourceMap = new HashMap<String, String>();
		sourceMap.put("url", url);
		sourceMap.put("driver", driver);
		sourceMap.put("user", user);
		sourceMap.put("password", password);
		sourceMap.put("dbtable", dbtable);
	//	sourceMap.put("numPartitions", noOfPartitions);
		//System.out.println("***** sourceMap: " + sourceMap);
		//df = sqlContext.load("jdbc", sourceMap);
		df = spark.read().format("jdbc").options(sourceMap).load();
//		df.show();
		return df;
	}
	public static Dataset<Row> createDataFrameFromFiles(JSONObject inputJSON, SparkSession spark) throws Exception{
		// TODO Auto-generated method stub
		Dataset<Row> df = null;
		boolean success = false;
		String fileType = inputJSON.getString("file-type");
		if(fileType.equalsIgnoreCase("parquet"))
			df = createDataFrameFromParquet(inputJSON, spark);
		else if(fileType.equalsIgnoreCase("json")) 
			df = createDataFrameFromJSON(inputJSON, spark);
		else if(fileType.equalsIgnoreCase("csv"))
			df = createDataFrameFromCSV(inputJSON, spark);
		else{ 
				LOG.error("Unsupported file type in input"); 
				System.exit(4);
		}	

		return df;
	}
	
}
