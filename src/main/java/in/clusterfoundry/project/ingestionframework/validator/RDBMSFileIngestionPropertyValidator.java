package in.clusterfoundry.project.ingestionframework.validator;

import java.util.Properties;

import org.apache.spark.sql.SparkSession;

import in.clusterfoundry.project.ingestionframework.constant.SourcePropertyConstant;
import in.clusterfoundry.project.ingestionframework.exceptions.IngestionException;
import in.clusterfoundry.project.ingestionframework.vo.IngestionSourcePropertyVO;

public class RDBMSFileIngestionPropertyValidator extends IngestionSourcePropertyValidator {

	public RDBMSFileIngestionPropertyValidator(IngestionSourcePropertyVO sourcePropertyVO) {
		super(sourcePropertyVO);
	}

	@Override
	public void validate() throws Exception {
		String url = sourcePropertyVO.getProperty(SourcePropertyConstant.RDBMS_URL);
		String user = sourcePropertyVO.getProperty(SourcePropertyConstant.RDBMS_USER);
		String pwd = sourcePropertyVO.getProperty(SourcePropertyConstant.RDBMS_PWD);
		String table = sourcePropertyVO.getProperty(SourcePropertyConstant.RDBMS_TABLE);

		validateRDBMS(url, user, pwd, table);
	}

	private void validateRDBMS(String url, String user, String pwd, String table) throws IngestionException {
		try {
			SparkSession spark = SparkSession.builder().master("local").appName("RDBMS File Download").getOrCreate();

			Properties props = new Properties();
			props.setProperty("user", user);
			props.setProperty("password", pwd);

			spark.read().jdbc(url, table, props);
			System.out.println("Validating RDBMS connection successfully");
		} catch (Exception e) {
			throw new IngestionException(e);
		}
	}
}
