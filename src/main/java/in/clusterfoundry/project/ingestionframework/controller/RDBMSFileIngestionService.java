package in.clusterfoundry.project.ingestionframework.controller;

import java.util.Properties;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import in.clusterfoundry.project.ingestionframework.constant.SourcePropertyConstant;
import in.clusterfoundry.project.ingestionframework.validator.IngestionSourcePropertyValidator;
import in.clusterfoundry.project.ingestionframework.vo.IngestionSourcePropertyVO;

public class RDBMSFileIngestionService extends IngestionService {
	public RDBMSFileIngestionService(IngestionSourcePropertyVO ingestionSourcePropertyVO,
			IngestionSourcePropertyValidator ingestionSourcePropertyValidator) throws Exception {
		super(ingestionSourcePropertyVO, ingestionSourcePropertyValidator);
	}

	@Override
	public void ingest() {
		IngestionSourcePropertyVO ingestionSourcePropertyVO = getIngestionSourcePropertyVO();
		String url = ingestionSourcePropertyVO.getProperty(SourcePropertyConstant.RDBMS_URL);
		String user = ingestionSourcePropertyVO.getProperty(SourcePropertyConstant.RDBMS_USER);
		String password = ingestionSourcePropertyVO.getProperty(SourcePropertyConstant.RDBMS_PWD);
		String table = ingestionSourcePropertyVO.getProperty(SourcePropertyConstant.RDBMS_TABLE);
		String outputFormat = ingestionSourcePropertyVO.getProperty(SourcePropertyConstant.OUTPUT_FORMAT);
		String outputPath = ingestionSourcePropertyVO.getProperty(SourcePropertyConstant.OUTPUT_PATH);

		downloadFromRDBMS(url, user, password, table, outputFormat, outputPath);
	}

	private void downloadFromRDBMS(String url, String user, String password, String table, String outputFormat,
			String downloadPath) {
		SparkSession spark = SparkSession.builder().master("local").appName("RDBMS File Download").getOrCreate();

		Properties props = new Properties();
		props.setProperty("user", user);
		props.setProperty("password", password);

		Dataset<Row> sqlDF = spark.read().jdbc(url, table, props);

		String outputPutFile = downloadPath + "/" + table + "_" + outputFormat;

		try {
			sqlDF.createTempView(table);
			sqlDF.write().format(outputFormat).save(outputPutFile);
			System.out.println("\nSuccessfully write Table:" + table + " to Path:" + outputPutFile + "\n");
		} catch (AnalysisException e) {
			e.printStackTrace();
		}
	}
}
