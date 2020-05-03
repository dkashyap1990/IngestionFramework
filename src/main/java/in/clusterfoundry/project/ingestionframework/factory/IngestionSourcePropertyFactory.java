package in.clusterfoundry.project.ingestionframework.factory;

import java.util.Map;

import in.clusterfoundry.project.ingestionframework.constant.SourcePropertyConstant;
import in.clusterfoundry.project.ingestionframework.exceptions.IngestionException;
import in.clusterfoundry.project.ingestionframework.utils.PropertyUtils;
import in.clusterfoundry.project.ingestionframework.vo.CloudFileIngestionPropertyVO;
import in.clusterfoundry.project.ingestionframework.vo.IngestionSourcePropertyVO;
import in.clusterfoundry.project.ingestionframework.vo.LocalFileIngestionPropertyVO;
import in.clusterfoundry.project.ingestionframework.vo.RDBMSFileIngestionPropertyVO;

public class IngestionSourcePropertyFactory {
	public static IngestionSourcePropertyVO getIngestionSourceProperty(String propertyFile) throws IngestionException {
		Map<String, String> propertyMap = PropertyUtils.getPropertyMap(propertyFile);
		IngestionSourcePropertyVO ingestionSource = null;
		System.out.println("Initializing with Property file : " + propertyFile);
		if (propertyMap != null) {
			String sourceType = propertyMap.get(SourcePropertyConstant.SOURCE_TYPE);

			switch (sourceType) {
			case SourcePropertyConstant.LFS_SOURCE:
				String sourcePath = propertyMap.get(SourcePropertyConstant.LFS_PATH);
				String destinationPath = propertyMap.get(SourcePropertyConstant.HDFS_PATH);
				ingestionSource = new LocalFileIngestionPropertyVO(sourcePath, destinationPath);
				break;

			case SourcePropertyConstant.CLOUD_SOURCE:
				String bucketName = propertyMap.get(SourcePropertyConstant.BUCKET_NAME);
				String objectName = propertyMap.get(SourcePropertyConstant.OBJECT_NAME);
				String downloadPath = propertyMap.get(SourcePropertyConstant.DOWNLOAD_PATH);
				ingestionSource = new CloudFileIngestionPropertyVO(bucketName, objectName, downloadPath);
				break;

			case SourcePropertyConstant.RDBMS_SOURCE:
				String url = propertyMap.get(SourcePropertyConstant.RDBMS_URL);
				String user = propertyMap.get(SourcePropertyConstant.RDBMS_USER);
				String password = propertyMap.get(SourcePropertyConstant.RDBMS_PWD);
				String table = propertyMap.get(SourcePropertyConstant.RDBMS_TABLE);
				String outputFormat = propertyMap.get(SourcePropertyConstant.OUTPUT_FORMAT);
				String outputPath = propertyMap.get(SourcePropertyConstant.OUTPUT_PATH);
				ingestionSource = new RDBMSFileIngestionPropertyVO(url, user, password, table, outputFormat,
						outputPath);
				break;

			}
		}

		return ingestionSource;
	}
}
