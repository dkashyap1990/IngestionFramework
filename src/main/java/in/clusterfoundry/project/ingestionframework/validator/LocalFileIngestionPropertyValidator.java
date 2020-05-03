package in.clusterfoundry.project.ingestionframework.validator;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import in.clusterfoundry.project.ingestionframework.constant.ExceptionMessageConstants;
import in.clusterfoundry.project.ingestionframework.constant.SourcePropertyConstant;
import in.clusterfoundry.project.ingestionframework.exceptions.IngestionException;
import in.clusterfoundry.project.ingestionframework.vo.IngestionSourcePropertyVO;

public class LocalFileIngestionPropertyValidator extends IngestionSourcePropertyValidator {

	public LocalFileIngestionPropertyValidator(IngestionSourcePropertyVO sourcePropertyVO) {
		super(sourcePropertyVO);
	}

	@Override
	public void validate() throws IngestionException, IOException {
		String source = sourcePropertyVO.getProperty(SourcePropertyConstant.LFS_PATH);
		String destination = sourcePropertyVO.getProperty(SourcePropertyConstant.HDFS_PATH);

		valildateLocalPath(source);
		validateHDFSPath(destination);
	}

	private void valildateLocalPath(String localPath) throws IngestionException {
		File file = new File(localPath);
		if (!file.exists()) {
			throw new IngestionException("Path # " + localPath + " : " + ExceptionMessageConstants.PATH_NOT_EXISTS);
		}
		System.out.println("Validating path : " + localPath + " successfully");
	}

	private void validateHDFSPath(String hdfsPath) throws IngestionException, IOException {

		FileSystem hdfs = FileSystem.get(new Configuration());

		if (!hdfs.exists(new Path(hdfsPath))) {
			throw new IngestionException("Path # " + hdfsPath + " : " + ExceptionMessageConstants.PATH_NOT_EXISTS);
		}

		System.out.println("Validating path : " + hdfsPath + " successfully");
	}
}
