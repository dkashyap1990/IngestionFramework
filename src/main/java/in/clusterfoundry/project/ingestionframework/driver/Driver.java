package in.clusterfoundry.project.ingestionframework.driver;

import in.clusterfoundry.project.ingestionframework.controller.IngestionService;
import in.clusterfoundry.project.ingestionframework.factory.IngestionServiceFactory;
import in.clusterfoundry.project.ingestionframework.factory.IngestionSourcePropertyFactory;
import in.clusterfoundry.project.ingestionframework.factory.IngestionSourcePropertyValidatorFactory;
import in.clusterfoundry.project.ingestionframework.validator.IngestionSourcePropertyValidator;
import in.clusterfoundry.project.ingestionframework.vo.IngestionSourcePropertyVO;

public class Driver {
	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.out.println("Usage : spark-submit <JAR><CONFIG FILE>");
			System.exit(-1);
		}

		String propertyFile = args[0];
		IngestionSourcePropertyVO source = IngestionSourcePropertyFactory.getIngestionSourceProperty(propertyFile);

		System.out.println(source);

		IngestionSourcePropertyValidator validator = IngestionSourcePropertyValidatorFactory.getValidator(source);
		IngestionService service = IngestionServiceFactory.getIngestionService(source, validator);
		service.ingest();
	}
}
