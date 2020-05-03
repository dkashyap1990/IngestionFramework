package in.clusterfoundry.project.ingestionframework.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import in.clusterfoundry.project.ingestionframework.exceptions.IngestionException;

public class PropertyUtils {
	public static Map<String, String> getPropertyMap(String propertyFile) throws IngestionException {
		Map<String, String> propertiesMap = null;

		Properties properties = new Properties();
		try {
			properties.load(new FileInputStream(propertyFile));
			propertiesMap = new HashMap<>();

			for (Object key : properties.keySet()) {
				propertiesMap.put(key.toString(), properties.get(key).toString());
			}

		} catch (FileNotFoundException exception) {
			throw new IngestionException(exception);
		} catch (IOException exception) {
			throw new IngestionException(exception);
		}

		return propertiesMap;
	}
}
