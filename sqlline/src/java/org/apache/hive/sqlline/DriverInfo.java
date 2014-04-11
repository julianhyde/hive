package org.apache.hive.sqlline;

import java.io.IOException;
import java.util.Properties;

public class DriverInfo {
  public String sampleURL;

  public DriverInfo(String name) throws IOException {
    Properties props = new Properties();
    props.load(DriverInfo.class.getResourceAsStream(name));
    fromProperties(props);
  }

  public DriverInfo(Properties props) {
    fromProperties(props);
  }

  public void fromProperties(Properties props) {
  }
}
