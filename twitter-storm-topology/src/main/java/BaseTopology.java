import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by mserrate on 26/12/15.
 */
public abstract class BaseTopology {
    private static final Logger LOG = LoggerFactory.getLogger(BaseTopology.class);

    protected Properties topologyConfig;

    public BaseTopology(String configFileLocation) throws Exception {

        topologyConfig = new Properties();
        try {
            InputStream is = new FileInputStream(configFileLocation);
            topologyConfig.load(is);
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
}