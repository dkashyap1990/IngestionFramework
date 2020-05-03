package in.clusterfoundry.project.ingestionframework.factory;

import in.clusterfoundry.project.ingestionframework.constant.SourcePropertyConstant;
import in.clusterfoundry.project.ingestionframework.validator.CloudFileIngestionPropertyValidator;
import in.clusterfoundry.project.ingestionframework.validator.IngestionSourcePropertyValidator;
import in.clusterfoundry.project.ingestionframework.validator.LocalFileIngestionPropertyValidator;
import in.clusterfoundry.project.ingestionframework.validator.RDBMSFileIngestionPropertyValidator;
import in.clusterfoundry.project.ingestionframework.vo.IngestionSourcePropertyVO;

public class IngestionSourcePropertyValidatorFactory {
	public static IngestionSourcePropertyValidator getValidator(IngestionSourcePropertyVO sourcePropertyVO) {
		IngestionSourcePropertyValidator validator = null;
		
		if (sourcePropertyVO != null) {
			
			String sourceType = sourcePropertyVO.getSourceType();
			switch (sourceType) {
			case SourcePropertyConstant.LFS_SOURCE:
				validator = new LocalFileIngestionPropertyValidator(sourcePropertyVO);
				break;
			case SourcePropertyConstant.CLOUD_SOURCE:
				validator = new CloudFileIngestionPropertyValidator(sourcePropertyVO);
				break;
			case SourcePropertyConstant.RDBMS_SOURCE:
				validator = new RDBMSFileIngestionPropertyValidator(sourcePropertyVO);
				break;
			}
		}

		return validator;
	}
}
