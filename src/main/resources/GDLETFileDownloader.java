package in.clusterfoundry.project.ingestionframework.service;

import java.io.File;

import in.clusterfoundry.project.ingestionframework.constant.InputConstant;
import in.clusterfoundry.project.ingestionframework.exception.IngestionException;
import in.clusterfoundry.project.ingestionframework.utils.CmdUtils;

public class GDLETFileDownloader {
	private String searchDate;

	public GDLETFileDownloader(String searchDate) {
		this.searchDate = searchDate;
	}

	public void download() throws IngestionException {
		CmdUtils.execute(new File(InputConstant.GDLET_SCRIPT), searchDate);
	}
}
