package in.clusterfoundry.project.ingestionframework.utils;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import in.clusterfoundry.project.ingestionframework.exceptions.IngestionException;

public class UnixTool {
	public static Process execute(String... commands) throws IngestionException {
		try {
			StringBuilder execCmds = new StringBuilder();
			for (String cmd : commands) {
				execCmds.append(cmd + " ");
			}
			System.out.println("Executing command : " + execCmds);
			return Runtime.getRuntime().exec(commands);
		} catch (IOException e) {
			throw new IngestionException(e);
		}
	}

	public static Process execute(File scriptFile) throws IngestionException {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(scriptFile));
			String command = "bash";
			String firstLine = reader.readLine();

			if (firstLine.contains("#")) {
				String[] cmd = firstLine.split("[/]+");
				String mainCmd = cmd[cmd.length - 1];
				command = mainCmd;
			}
			reader.close();
			return execute(command, scriptFile.toString());
		} catch (Exception e) {
			throw new IngestionException(e);
		}
	}

	public static List<String> readProcessOutput(Process p) throws IngestionException {
		List<String> output = new ArrayList<>();
		try {
			BufferedReader buffer = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line = null;
			while ((line = buffer.readLine()) != null) {
				output.add(line);
				System.out.println(line);
			}
			buffer.close();
		} catch (Exception e) {
			throw new IngestionException(e);
		}
		return output;
	}

	public static void main(String[] args) throws IngestionException {
		Process p = execute(new File("/home/deepjyoti/Projects/scripts/test1.sh"));
		readProcessOutput(p);
	}
}
