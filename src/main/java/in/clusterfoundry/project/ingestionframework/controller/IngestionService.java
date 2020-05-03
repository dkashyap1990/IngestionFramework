package in.clusterfoundry.project.ingestionframework.controller;

import in.clusterfoundry.project.ingestionframework.constant.ExceptionMessageConstants;
import in.clusterfoundry.project.ingestionframework.exceptions.IngestionException;
import in.clusterfoundry.project.ingestionframework.validator.IngestionSourcePropertyValidator;
import in.clusterfoundry.project.ingestionframework.vo.IngestionSourcePropertyVO;

public abstract class IngestionService {
	private IngestionSourcePropertyVO ingestionSourcePropertyVO;
	private IngestionSourcePropertyValidator ingestionSourcePropertyValidator;

	public IngestionService(IngestionSourcePropertyVO ingestionSourcePropertyVO,
			IngestionSourcePropertyValidator ingestionSourcePropertyValidator) throws Exception {
		if (ingestionSourcePropertyVO == null) {
			throw new IngestionException(ExceptionMessageConstants.INVALID_INGESTION_VO);
		}

		if (ingestionSourcePropertyValidator == null) {
			throw new IngestionException(ExceptionMessageConstants.INVALID_VALIDATOR);
		}

		this.ingestionSourcePropertyVO = ingestionSourcePropertyVO;
		this.ingestionSourcePropertyValidator = ingestionSourcePropertyValidator;
		System.out.println("Initializing service : " + this.getClass().getSimpleName());
		this.ingestionSourcePropertyValidator.validate();
	}

	public IngestionSourcePropertyVO getIngestionSourcePropertyVO() {
		return ingestionSourcePropertyVO;
	}

	public IngestionSourcePropertyValidator getIngestionSourcePropertyValidator() {
		return ingestionSourcePropertyValidator;
	}

	public abstract void ingest() throws Exception;
}
