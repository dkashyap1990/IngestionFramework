package in.clusterfoundry.project.ingestionframework.validator;

import java.io.File;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BucketListOption;
import com.google.cloud.storage.StorageOptions;

import in.clusterfoundry.project.ingestionframework.constant.ExceptionMessageConstants;
import in.clusterfoundry.project.ingestionframework.constant.SourcePropertyConstant;
import in.clusterfoundry.project.ingestionframework.exceptions.IngestionException;
import in.clusterfoundry.project.ingestionframework.vo.IngestionSourcePropertyVO;

public class CloudFileIngestionPropertyValidator extends IngestionSourcePropertyValidator {
	public CloudFileIngestionPropertyValidator(IngestionSourcePropertyVO sourcePropertyVO) {
		super(sourcePropertyVO);
	}

	@Override
	public void validate() throws IngestionException {
		String bucketName = sourcePropertyVO.getProperty(SourcePropertyConstant.BUCKET_NAME);
		String objectName = sourcePropertyVO.getProperty(SourcePropertyConstant.OBJECT_NAME);
		String downloadPath = sourcePropertyVO.getProperty(SourcePropertyConstant.DOWNLOAD_PATH);

		validateBucketAndObjectName(bucketName, objectName);
		validateDownloadPath(downloadPath);
	}

	private void validateBucketAndObjectName(String bucketName, String blobName) throws IngestionException {
		Storage storage = StorageOptions.getDefaultInstance().getService();

		Page<Bucket> buckets = storage.list(BucketListOption.pageSize(100));
		boolean isBucketFound = false;

		for (Bucket bucket : buckets.iterateAll()) {
			if (bucket.getName().equals(bucketName)) {
				isBucketFound = true;
				break;
			}
		}

		if (!isBucketFound)
			throw new IngestionException(ExceptionMessageConstants.INVALID_BUCKET);

		BlobId blobId = BlobId.of(bucketName, blobName, null);
		Blob blob = storage.get(blobId);
		if (blob == null) {
			throw new IngestionException(ExceptionMessageConstants.INVALID_BLOB);
		}
		System.out.println("Validating Bucket : " + bucketName + " successfully");
		System.out.println("Validating ObjectName : " + blobName + " successfully");
	}

	private void validateDownloadPath(String downloadPath) throws IngestionException {
		File file = new File(downloadPath);
		if (!file.exists()) {
			throw new IngestionException("Path # " + downloadPath + " : " + ExceptionMessageConstants.PATH_NOT_EXISTS);
		}
		System.out.println("Validating path : " + downloadPath + " successfully");
	}
}
