package in.clusterfoundry.project.ingestionframework.vo;

import java.util.HashMap;
import java.util.Map;

import in.clusterfoundry.project.ingestionframework.constant.SourcePropertyConstant;

public class LocalFileIngestionPropertyVO extends IngestionSourcePropertyVO {
	public LocalFileIngestionPropertyVO(String sourcePath, String destinationPath) {
		Map<String, String> sourceProperties = new HashMap<>();
		sourceProperties.put(SourcePropertyConstant.LFS_PATH, sourcePath);
		sourceProperties.put(SourcePropertyConstant.HDFS_PATH, destinationPath);

		setSourceType(SourcePropertyConstant.LFS_SOURCE);
		setSourceProperties(sourceProperties);
	}
}
