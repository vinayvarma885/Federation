package com.gathi.mdm.federation;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.esotericsoftware.minlog.Log;
import com.gathi.mdm.utils.configManagement.ConfigManager;

public class Preprocessor {

	final static Logger LOG = Logger.getLogger(Preprocessor.class);

	public static JSONObject process(JSONObject federationJSONConfig, String application_id) {

		// Step 1 : SQL pre-processing
		federationJSONConfig = processSQL(federationJSONConfig);

		// Step 2 : Input pre-processing
		federationJSONConfig = processInputs(federationJSONConfig);

		// Step 3 : Output pre-processing
		federationJSONConfig = processOutputs(federationJSONConfig);

		// Step 4 : generate aliases for inputs and create an executable sql
		federationJSONConfig = generateAliasesAndExecuatableSQL(federationJSONConfig, application_id);
		return federationJSONConfig;
	}

	private static JSONObject processSQL(JSONObject federationJSONConfig) {
		String sql = null;
		try {
			sql = federationJSONConfig.getString("sql");
			// System.out.println("sql:"+sql);
		} catch (JSONException e) {
			LOG.error("Mandatory Key 'sql' not found in config file");
			System.exit(4);
		}

		if (sql.length() == 0) {
			LOG.error("Config file has no value for key 'sql'");
			System.exit(4);
		}
		try {
			federationJSONConfig.put("orignal-sql", sql);
		} catch (Exception e) {
			LOG.error("Query Validation failed", e);
			System.exit(1);
		}
		// uncomment query
		sql = QueryUtils.unCommentQuery(sql);
		// System.out.println("returnsql"+sql);
		// check if dynamic conditions are present and impute
		int numberOfPlaceHoldersInSQL = QueryUtils.getDynamicConditionsCount(sql);
		// System.out.println("numberOfPlaceHolders"+numberOfPlaceHoldersInSQL);
		if (numberOfPlaceHoldersInSQL > 0) {

			// System.out.println("inside placeholders");
			JSONArray dynamicConditions = null;
			try {
				dynamicConditions = federationJSONConfig.getJSONArray("dynamic-conditions");
			} catch (JSONException e) {
				LOG.error("Found placeholders for dynamic imputation in specified SQL query,"
						+ " but no values for these condiitons have been specified using the Key 'dynamic-conditions'."
						+ "Please specify a JSONArray of values for the dynamic conditions.", e);
				System.exit(4);
			}
			if (numberOfPlaceHoldersInSQL != dynamicConditions.length()) {
				LOG.error(String.format(
						"Number of placeholders {} found in SQL do not match the number of dynamic-conditions {} specified in config file.",
						numberOfPlaceHoldersInSQL, dynamicConditions.length()));
				System.exit(4);
			}

			sql = QueryUtils.imputeDynamicConditions(dynamicConditions, sql);

			try {
				federationJSONConfig.put("sql", sql);
			} catch (JSONException e) {
				LOG.error("Preprocessor failed with", e);
				System.exit(1);
			}
		}

		// validate query safety
		boolean querySafety = QueryUtils.validateQuerySafety(sql);
		// System.out.println("querysafety"+querySafety);

		if (querySafety == false) {
			// System.out.println("inside validtequery");
			LOG.error(String.format("Unsafe Query. Query with %s are considered unsafe.",
					String.join(", ", FederationConfigs.UNSAFE_OPERATIONS)));
			System.exit(4);
		}
		// System.out.println("federationJSONConfig:"+federationJSONConfig.toString());
		return federationJSONConfig;
	}

	private static JSONObject processInputs(JSONObject federationJSONConfig) {
		// atleast 1 input
		JSONArray sources = null;
		try {
			sources = federationJSONConfig.getJSONArray("inputs");
		} catch (JSONException e) {
			LOG.error("Mandatory Key 'inputs' not found in config file");
			System.exit(4);
		}
		if (sources.length() < 1) {
			LOG.warn("No 'inputs' specified.");
			// System.exit(4);
		}
		// validate input and add configs
		boolean allInputsAreValid = true;
		ConfigManager configManager = new ConfigManager();

		for (int i = 0; i < sources.length(); i++) {
			JSONObject source = null;
			String sourceName = null;
			String userOrStar = "*";

			try {
				source = sources.getJSONObject(i);
			} catch (Exception e) {
				LOG.error(String.format("Failed to read source %s from 'inputs' in provided config file", i));
				System.exit(4);
			}
			try {
				sourceName = source.getString("datasource");
				// System.out.println("sources"+sourceName);
			} catch (JSONException e) {
				LOG.error(
						String.format("Mandatory Key 'datasource' not found for input %s. Correct the configFile", i));
				System.exit(4);
			}
			if (sourceName.equals("FILES")) {
				JSONObject type = new JSONObject();

				try {
					type.put("type", "FILES");
					source.put("config", type);
					sources.put(i, source);
					System.out.println("sources:" + sources.toString());

				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			} else if (sourceName.equals("HDFS")) {
				JSONObject type = new JSONObject();

				try {
					type.put("type", "HDFS");
					source.put("config", type);
					sources.put(i, source);
					System.out.println("sources:" + sources.toString());

				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				try {
					userOrStar = source.getString("user");
				} catch (JSONException e) {
					LOG.error(String.format("Mandatory Key 'user' not found for input %s. Correct the configFile", i));
					System.exit(4);
				}
				if (configManager.isSourceValid(System.getProperty("ENV"), sourceName) == true) {
					String databaseOrStar = "*";
					try {
						if (source.has("database")) {
							databaseOrStar = source.getString("database");
						}
					} catch (Exception e) {
						Log.error("**Preprocessor failed with ", e);
						System.exit(1);
					}
					try {
						// System.out.println("config " +
						// configManager.getConfig(System.getProperty("ENV"), sourceName,
						// databaseOrStar, userOrStar).toString());

						source.put("config", configManager.getConfig(System.getProperty("ENV"), sourceName,
								databaseOrStar, userOrStar));
						sources.put(i, source);

					} catch (JSONException e) {
						LOG.error("Preprocessor failed with : ", e);
						System.exit(1);
					}
				} else {
					LOG.error("Unrecogonised Source Name : " + sourceName + " in 'inputs'");
					allInputsAreValid = false;
				}
			}

		}
		if (allInputsAreValid == false) {
			LOG.error("One or more of the sources in 'inputs' are unsupported");
			System.exit(4);
		}
		try {
			federationJSONConfig.put("inputs", sources);
		} catch (JSONException e) {
			LOG.error("Preprocessor failed with : ", e);
			System.exit(1);
		}
		// System.out.println("config "+federationJSONConfig.toString());
		return federationJSONConfig;
	}

	private static JSONObject processOutputs(JSONObject federationJSONConfig) {
		JSONArray outputs = null;
		try {
			outputs = federationJSONConfig.getJSONArray("outputs");
		} catch (JSONException e) {
			LOG.error("Mandatory Key 'outputs' not found in configFile");
			System.exit(4);
		}
		if (outputs.length() < 1) {
			LOG.error("Config file has no values for key 'outputs'. Atleast one output must be specified.");
			System.exit(4);
		}

		// validate output and add configs
		boolean allOutputsAreValid = true;
		ConfigManager configManager = new ConfigManager();
		for (int i = 0; i < outputs.length(); i++) {
			JSONObject output = null;
			String outputName = null;
			String userOrStar = "*";
			try {
				output = outputs.getJSONObject(i);
			} catch (Exception e) {
				LOG.error(String.format("Failed to read source %s from 'outputs' in provided config file", i));
				System.exit(4);
			}
			try {
				outputName = output.getString("datasink");
			} catch (JSONException e) {
				LOG.error(String.format("Mandatory Key 'datasink' not found for output %s. Correct the configFile", i));
				System.exit(4);
			}
			try {
				userOrStar = output.getString("user");
			} catch (JSONException e) {
				LOG.error(String.format("Mandatory Key 'user' not found for output %s. Correct the configFile", i));
				System.exit(4);
			}
			if (configManager.isSourceValid(System.getProperty("ENV"), outputName) == true) {
				String DatabaseOrStar = "*";
				try {
					if (output.has("database")) {
						DatabaseOrStar = output.getString("database");
					}
				} catch (Exception e) {
					LOG.error("**Preprocessor failed with : ", e);
					System.exit(1);
				}
				try {
					// System.out.println("config " +
					// configManager.getConfig(System.getProperty("ENV"), outputName,
					// DatabaseOrStar, userOrStar).toString());
					output.put("config",
							configManager.getConfig(System.getProperty("ENV"), outputName, DatabaseOrStar, userOrStar));
					outputs.put(i, output);
				} catch (JSONException e) {
					LOG.error("Preprocessor failed with : ", e);
					System.exit(1);
				}
			} else {
				LOG.error("Found unsupported output type: " + outputName + " in 'outputs'");
				allOutputsAreValid = false;
			}
		}
		if (allOutputsAreValid == false) {
			LOG.error("One or more of the output in 'outputs' are unsupported");
			System.exit(4);
		}
		try {
			federationJSONConfig.put("outputs", outputs);
		} catch (JSONException e) {
			LOG.error("Preprocessor failed with : ", e);
			System.exit(1);
		}
		// System.out.println("config1"+federationJSONConfig.toString());

		return federationJSONConfig;
	}

	private static JSONObject generateAliasesAndExecuatableSQL(JSONObject federationJSONConfig, String application_id) {
		JSONArray sources = null;
		final String SPACE = " ";
		try {
			sources = federationJSONConfig.getJSONArray("inputs");
		} catch (JSONException e) {
			LOG.error("Mandatory Key 'inputs' not found in config file");
			System.exit(4);
		}

		String sql = null;
		try {
			sql = federationJSONConfig.getString("sql");
		} catch (JSONException e) {
			LOG.error("Mandatory Key 'sql' not found in config file");
			System.exit(4);
		}
		// generate aliases and executable sql
		ConfigManager configs = new ConfigManager();
		String executableSQL = sql;
		try {
			for (int i = 0; i < sources.length(); i++) {
				JSONObject source = sources.getJSONObject(i);
				String sourceName = source.getString("datasource");
				String placeHolder = "";
				System.out.println("source " + source);
				if (source.has("database")) {
					// data source is a database
					String database = source.getString("database");
					String schema = null;
					if (source.has("schema"))
						schema = source.getString("schema");
					String table = source.getString("table");

					String placeHolderWithSchema = "%s.%s.%s.%s";
					String placeHolderWithoutSchema = "%s.%s.%s";
					if (schema != null)
						placeHolder = String.format(placeHolderWithSchema, sourceName, database, schema, table);
					else
						placeHolder = String.format(placeHolderWithoutSchema, sourceName, database, table);

				}

				if (source.has("file-path") && source.getString("datasource").equals("S3")) {
					// source is a file
					System.out.println("hellos3");
					Path path = new Path(source.getString("file-path"));
					String fileName1 = path.getName();
					placeHolder = source.getString("file-path").replace("s3a://", "S3.").replace("/", ".")
							.replace(fileName1, fileName1);
					String sourceAlias = "";

					sourceAlias = placeHolder.replaceAll("\\.", "_") + "_" + application_id.replace("-", "_");
					sourceAlias = sourceAlias.replaceAll("\\.", "_");
					sourceAlias = sourceAlias.replaceAll("-", "_");
					sourceAlias = SPACE + sourceAlias + SPACE;

					source.put("alias", sourceAlias);
					String searchPattern = "\\.*?\\b" + placeHolder + "\\b.*?";
					System.out.println("searchPattern " + searchPattern);
					System.out.println("sourceAlias " + sourceAlias);

					System.out.println("executableSQL:" + executableSQL);
					// System.out.println("\n\n>> " + searchPattern + "\n" + sourceAlias+"\n");
					// System.out.println("Place holder : " + placeHolder + "\nSearchPattern : " +
					// searchPattern + "\n sourceAlias : " + sourceAlias);
					// executableSQL=executableSQL.replaceFirst("FILES", "FILES.home.hduser");
					executableSQL = executableSQL.replaceAll(searchPattern, sourceAlias);
				} else if (source.has("file-path") && source.getString("datasource").equals("FILES")) {
					System.out.println("files");
					Path path = new Path(source.getString("file-path"));
					String fileName1 = path.getName();
					System.out.println("filename" + fileName1);
					placeHolder = source.getString("file-path").replace("file://", "FILES").replace("/", ".")
							.replace(fileName1, fileName1);
					System.out.println("placeholder" + placeHolder);
					String sourceAlias = "";

					sourceAlias = placeHolder.replaceAll("\\.", "_") + "_" + application_id.replace("-", "_");
					sourceAlias = sourceAlias.replaceAll("\\.", "_");
					sourceAlias = sourceAlias.replaceAll("-", "_");
					sourceAlias = SPACE + sourceAlias + SPACE;

					source.put("alias", sourceAlias);
					String searchPattern = "\\.*?\\b" + placeHolder + "\\b.*?";
					System.out.println("searchPattern " + searchPattern);
					System.out.println("sourceAlias " + sourceAlias);

					System.out.println("executableSQL:" + executableSQL);
					executableSQL = executableSQL.replaceFirst("FILES", "FILES.home.hduser");
					executableSQL = executableSQL.replaceAll(searchPattern, sourceAlias);
				}
				else if(source.has("file-path") && source.getString("datasource").equals("HDFS"))
				{
					System.out.println("HDFS");
					Path path = new Path(source.getString("file-path"));
					String fileName1 = path.getName();
					System.out.println("filename" + fileName1);
					placeHolder = source.getString("file-path").replace("hdfs://", "HDFS").replace("/", ".")
							.replace(fileName1, fileName1);
					System.out.println("placeholder" + placeHolder);
					String sourceAlias = "";

					sourceAlias = placeHolder.replaceAll("\\.", "_") + "_" + application_id.replace("-", "_");
					sourceAlias = sourceAlias.replaceAll("\\.", "_");
					sourceAlias = sourceAlias.replaceAll("-", "_");
					sourceAlias = SPACE + sourceAlias + SPACE;

					source.put("alias", sourceAlias);
					String searchPattern = "\\.*?\\b" + placeHolder + "\\b.*?";
					System.out.println("searchPattern " + searchPattern);
					System.out.println("sourceAlias " + sourceAlias);

					System.out.println("executableSQL:" + executableSQL);
					executableSQL = executableSQL.replaceFirst("HDFS", "HDFS");
					executableSQL = executableSQL.replaceAll(searchPattern, sourceAlias);
				}
				else {
					String sourceAlias = "";

					sourceAlias = placeHolder.replaceAll("\\.", "_") + "_" + application_id.replace("-", "_");
					sourceAlias = sourceAlias.replaceAll("\\.", "_");
					sourceAlias = sourceAlias.replaceAll("-", "_");
					sourceAlias = SPACE + sourceAlias + SPACE;

					source.put("alias", sourceAlias);
					String searchPattern = "\\.*?\\b" + placeHolder + "\\b.*?";
					System.out.println("searchPattern " + searchPattern);
					System.out.println("sourceAlias " + sourceAlias);

					System.out.println("executableSQL:" + executableSQL);
					// System.out.println("\n\n>> " + searchPattern + "\n" + sourceAlias+"\n");
					// System.out.println("Place holder : " + placeHolder + "\nSearchPattern : " +
					// searchPattern + "\n sourceAlias : " + sourceAlias);
					// executableSQL=executableSQL.replaceFirst("FILES", "FILES.home.hduser");
					executableSQL = executableSQL.replaceAll(searchPattern, sourceAlias);
				}
				

				// System.out.println("SQL : " + executableSQL);
				sources.put(i, source);
			}
			federationJSONConfig.put("inputs", sources);
			federationJSONConfig.put("executable-sql", executableSQL);
		} catch (JSONException e) {
			LOG.error("generateAliasesAndExecuatableSQL() failed with ", e);
			System.exit(1);
		}
		System.out.println("exec" + federationJSONConfig);
		return federationJSONConfig;
	}
}
