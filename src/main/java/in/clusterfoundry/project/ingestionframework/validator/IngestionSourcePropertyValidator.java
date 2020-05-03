package in.clusterfoundry.project.ingestionframework.validator;

import in.clusterfoundry.project.ingestionframework.vo.IngestionSourcePropertyVO;

public abstract class IngestionSourcePropertyValidator {
	protected IngestionSourcePropertyVO sourcePropertyVO;

	public IngestionSourcePropertyValidator(IngestionSourcePropertyVO sourcePropertyVO) {
		this.sourcePropertyVO = sourcePropertyVO;
		System.out.println("Initializing validator : " + this.getClass().getSimpleName());
	}

	public IngestionSourcePropertyVO getSourcePropertyVO() {
		return sourcePropertyVO;
	}

	public abstract void validate() throws Exception;
}
