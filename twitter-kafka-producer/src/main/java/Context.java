import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by mserrate on 14/12/15.
 */
public class Context {
    private static final Logger LOG = LoggerFactory.getLogger(Context.class);

    Properties properties;

    Context(String configFileLocation) throws Exception {
        properties = new Properties();
        try {
            InputStream is = new FileInputStream(configFileLocation);
            properties.load(is);
        } catch (FileNotFoundException e) {
            LOG.error("Encountered error while reading configuration properties: "
                    + e.getMessage());
            throw e;
        } catch (IOException e) {
            LOG.error("Encountered error while reading configuration properties: "
                    + e.getMessage());
            throw e;
        }
    }

    public String getString(String key) {
        return properties.getProperty(key);
    }
}