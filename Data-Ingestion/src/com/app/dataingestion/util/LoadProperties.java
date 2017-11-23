package com.app.dataingestion.util;
/**
 * @author naga
 *
 */
import java.io.IOException;
import java.util.Properties;

public class LoadProperties {
	
	private static LoadProperties instance = null;
	private Properties properties;
	
	private LoadProperties() throws IOException{
		properties = new Properties();
		properties.load(LoadProperties.class.getResourceAsStream("/props.properties"));

    }
    public static LoadProperties getInstance() {
        if(instance == null) {
            try {
                instance = new LoadProperties();
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
        return instance;
    }
    public String getValue(String key) {
        return properties.getProperty(key);
    }

}
