package in.clusterfoundry.project.ingestionframework.exceptions;

public class IngestionException extends Exception{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public IngestionException(String exception) {
		super(exception);
	}
	
	public IngestionException(Throwable exception) {
		super(exception);
	}
}
