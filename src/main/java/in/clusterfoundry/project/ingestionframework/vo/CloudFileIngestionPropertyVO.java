package in.clusterfoundry.project.ingestionframework.vo;

import java.util.HashMap;
import java.util.Map;

import in.clusterfoundry.project.ingestionframework.constant.SourcePropertyConstant;

public class CloudFileIngestionPropertyVO extends IngestionSourcePropertyVO {
	public CloudFileIngestionPropertyVO(String bucketName, String objectName, String downloadPath) {

		Map<String, String> sourceProperties = new HashMap<>();
		sourceProperties.put(SourcePropertyConstant.BUCKET_NAME, bucketName);
		sourceProperties.put(SourcePropertyConstant.OBJECT_NAME, objectName);
		sourceProperties.put(SourcePropertyConstant.DOWNLOAD_PATH, downloadPath);

		setSourceType(SourcePropertyConstant.CLOUD_SOURCE);
		setSourceProperties(sourceProperties);
	}
}
