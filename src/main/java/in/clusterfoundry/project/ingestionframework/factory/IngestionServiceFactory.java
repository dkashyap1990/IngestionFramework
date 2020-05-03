package in.clusterfoundry.project.ingestionframework.factory;

import in.clusterfoundry.project.ingestionframework.constant.SourcePropertyConstant;
import in.clusterfoundry.project.ingestionframework.controller.CloudFileIngestionService;
import in.clusterfoundry.project.ingestionframework.controller.IngestionService;
import in.clusterfoundry.project.ingestionframework.controller.LocalFileIngestionService;
import in.clusterfoundry.project.ingestionframework.controller.RDBMSFileIngestionService;
import in.clusterfoundry.project.ingestionframework.validator.IngestionSourcePropertyValidator;
import in.clusterfoundry.project.ingestionframework.vo.IngestionSourcePropertyVO;

public class IngestionServiceFactory {
	public static IngestionService getIngestionService(IngestionSourcePropertyVO ingestionSourcePropertyVO,
			IngestionSourcePropertyValidator ingestionSourcePropertyValidator) throws Exception {
		IngestionService service = null;

		if (ingestionSourcePropertyVO != null) {
			String sourceType = ingestionSourcePropertyVO.getSourceType();

			switch (sourceType) {
			case SourcePropertyConstant.LFS_SOURCE:
				service = new LocalFileIngestionService(ingestionSourcePropertyVO, ingestionSourcePropertyValidator);
				break;

			case SourcePropertyConstant.CLOUD_SOURCE:
				service = new CloudFileIngestionService(ingestionSourcePropertyVO, ingestionSourcePropertyValidator);
				break;

			case SourcePropertyConstant.RDBMS_SOURCE:
				service = new RDBMSFileIngestionService(ingestionSourcePropertyVO, ingestionSourcePropertyValidator);
				break;
			}
		}

		return service;
	}
}
