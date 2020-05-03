package in.clusterfoundry.project.ingestionframework.controller;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import in.clusterfoundry.project.ingestionframework.constant.SourcePropertyConstant;
import in.clusterfoundry.project.ingestionframework.exceptions.IngestionException;
import in.clusterfoundry.project.ingestionframework.validator.IngestionSourcePropertyValidator;
import in.clusterfoundry.project.ingestionframework.vo.IngestionSourcePropertyVO;

public class LocalFileIngestionService extends IngestionService {

	public LocalFileIngestionService(IngestionSourcePropertyVO ingestionSourcePropertyVO,
			IngestionSourcePropertyValidator ingestionSourcePropertyValidator) throws Exception {
		super(ingestionSourcePropertyVO, ingestionSourcePropertyValidator);
	}

	@Override
	public void ingest() throws IngestionException, IOException {
		IngestionSourcePropertyVO ingestionSourcePropertyVO = getIngestionSourcePropertyVO();
		String src = ingestionSourcePropertyVO.getProperty(SourcePropertyConstant.LFS_PATH);
		String dest = ingestionSourcePropertyVO.getProperty(SourcePropertyConstant.HDFS_PATH);

		moveDataFromLocalToHDFS(src, dest);
	}

	private void moveDataFromLocalToHDFS(String src, String dest) throws IngestionException, IOException {
		FileSystem hdfs;
		try {
			hdfs = FileSystem.get(new Configuration());
		} catch (IOException e) {
			throw new IngestionException(e);
		}

		Path localPath = new Path(src);
		Path hdfsPath = new Path(dest);

		File localFile = new File(localPath.toString());

		if (localFile.isDirectory()) {
			File[] filesList = localFile.listFiles();
			for (File file : filesList) {
				if (file.isFile()) {
					hdfs.copyFromLocalFile(new Path(file.getPath()), hdfsPath);
					System.out.println("Moving " + file.getPath() + " to " + dest + " successfully");
				}
			}
		} else {
			hdfs.copyFromLocalFile(localPath, hdfsPath);
			System.out.println("Moving " + src + " to " + dest + " successfully");
		}

		System.out.println("Moving " + src + " to " + dest + " successfully");
	}

}
