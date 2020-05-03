package in.clusterfoundry.project.ingestionframework;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import in.clusterfoundry.project.ingestionframework.exceptions.IngestionException;

public class Testing {
	public static void main(String[] args) throws Exception {

		String bucketName = "dataproc-f32b4463-3ff9-430a-a788-0c31cfc43b1b-asia-southeast1";
		String objectName = "Song.mp3";
		String downloadPath = "/home/deepjyoti/Projects/download";
		moveDataFromCloudTOLocal(bucketName, objectName, downloadPath);
	}

	private static void moveDataFromCloudTOLocal(String bucketName, String objectName, String downloadPath)
			throws Exception {
		Storage storage = StorageOptions.getDefaultInstance().getService();
		BlobId blobId = BlobId.of(bucketName, objectName, null);
		Blob blob = storage.get(blobId);

		if (blob == null) {
			System.out.println("No such object");
			return;
		}

		PrintStream writeTo = System.out;

		if (downloadPath != null) {
			writeTo = new PrintStream(new FileOutputStream(downloadPath));
		}

		if (blob.getSize() < 1_000_000) {
			// Blob is small read all its content in one request
			byte[] content = blob.getContent();
			try {
				writeTo.write(content);
			} catch (IOException e) {
				writeTo.close();
				throw new IngestionException(e);
			}
		} else {
			// When Blob size is big or unknown use the blob's channel reader.
			try (ReadChannel reader = blob.reader()) {
				WritableByteChannel channel = Channels.newChannel(writeTo);
				ByteBuffer bytes = ByteBuffer.allocate(64 * 1024);
				while (reader.read(bytes) > 0) {
					bytes.flip();
					channel.write(bytes);
					bytes.clear();
				}
			} catch (IOException e) {
				throw new IngestionException(e);
			}
		}
		writeTo.close();
		System.out.println("Moving " + objectName + " to " + downloadPath + " successfully");
	}

}
