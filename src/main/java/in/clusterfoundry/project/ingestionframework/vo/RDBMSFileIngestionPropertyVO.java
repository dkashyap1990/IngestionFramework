package in.clusterfoundry.project.ingestionframework.vo;

import java.util.HashMap;
import java.util.Map;

import in.clusterfoundry.project.ingestionframework.constant.SourcePropertyConstant;

public class RDBMSFileIngestionPropertyVO extends IngestionSourcePropertyVO {
	public RDBMSFileIngestionPropertyVO(String url, String user, String password, String table, String outputFormat,
			String outputPath) {

		Map<String, String> sourceProperties = new HashMap<>();
		sourceProperties.put(SourcePropertyConstant.RDBMS_URL, url);
		sourceProperties.put(SourcePropertyConstant.RDBMS_USER, user);
		sourceProperties.put(SourcePropertyConstant.RDBMS_PWD, password);
		sourceProperties.put(SourcePropertyConstant.RDBMS_TABLE, table);
		sourceProperties.put(SourcePropertyConstant.OUTPUT_FORMAT, outputFormat);
		sourceProperties.put(SourcePropertyConstant.OUTPUT_PATH, outputPath);

		setSourceType(SourcePropertyConstant.RDBMS_SOURCE);
		setSourceProperties(sourceProperties);
	}
}
