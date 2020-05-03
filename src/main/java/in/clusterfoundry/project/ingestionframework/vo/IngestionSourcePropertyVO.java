package in.clusterfoundry.project.ingestionframework.vo;

import java.util.Collections;
import java.util.Map;

public abstract class IngestionSourcePropertyVO {
	private String sourceType;
	private Map<String, String> sourceProperties;

	public IngestionSourcePropertyVO() {
		System.out.println("Initializing source : " + this.getClass().getSimpleName());
	}

	public String getSourceType() {
		return sourceType;
	}

	public Map<String, String> getSourceProperties() {
		return Collections.unmodifiableMap(sourceProperties);
	}

	@Override
	public String toString() {
		return "IngestionSourcePropertyVO [sourceType=" + sourceType + ", sourceProperties=" + sourceProperties + "]";
	}

	protected void setSourceType(String sourceType) {
		this.sourceType = sourceType;
	}

	protected void setSourceProperties(Map<String, String> sourceProperties) {
		this.sourceProperties = sourceProperties;
	}

	public String getProperty(String key) {
		if (sourceProperties != null) {
			return sourceProperties.get(key);
		}
		return null;
	}
}
