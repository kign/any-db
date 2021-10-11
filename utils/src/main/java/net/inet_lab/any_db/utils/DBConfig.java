package net.inet_lab.any_db.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.StringReader;
import java.nio.file.Paths;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.Enumeration;
import java.util.Collection;

public class DBConfig {
    public final String driver;
    public final String host;
    public final Integer port;
    public final String database;
    public final boolean ssl;
    public final String sslfactory;
    public final String default_user;

    public String getDriver() {
        return driver;
    }

    public String getHost () {
        return host;
    }

    public Integer getPort () {
        return port;
    }

    public String getDatabase () {
        return database;
    }

    public boolean getSsl() {
        return ssl;
    }

    public String getSslfactory () {
        return sslfactory;
    }

    public String getDefaultUser () {
        return default_user;
    }

    static final private Map<String,DBConfig> knownConfigs = new HashMap<String,DBConfig>() {{
        // built-in servers
        put("name", new DBConfig ("redshift", "XXX.redshift.amazonaws.com", 5439, "redshift_db"));
    }};

    static {
        try {
            String propFile = Paths.get(System.getProperty("user.home") , ".any-db").toString();
            InputStream input = new FileInputStream(propFile);
            Properties prop = new Properties();
            prop.load(input);
            Enumeration<String> enums = (Enumeration<String>) prop.propertyNames();
            while (enums.hasMoreElements()) {
                String name = enums.nextElement();
                String value = prop.getProperty(name);
                Properties np = new Properties();
                np.load(new StringReader(value.replaceAll(",", "\n")));
                String driver = np.getProperty("driver", "postgresql");
                String host = np.getProperty("host");
                if (host == null)
                    throw new RuntimeException("Error in file " + propFile + ", name \"" + name + "\" : " + "host must be defined");
                String port_s = np.getProperty("port");
                /*if (port_s == null)
                    throw new RuntimeException("Error in file " + propFile + ", name \"" + name + "\" : " + "port must be defined");*/
                Integer port = (port_s == null)? null: Integer.valueOf(port_s);
                String db = np.getProperty("database");
                if (db == null)
                    throw new RuntimeException("Error in file " + propFile + ", name \"" + name + "\" : " + "database must be defined");
                String user = np.getProperty("user");
                knownConfigs.put(name, new DBConfig(driver, host, port, db, false, null, user));
            }
        }
        catch (IOException ex) {
        }
    }

    static public DBConfig valueOf(String configName) {
        return knownConfigs.get(configName);
    }

    static public Collection<String> values () { return knownConfigs.keySet(); }

    static public DBConfig make(DBConfig deflt, String driver, String host, Integer port, String database) {
        if (host == null && port == null && database == null && driver == null) {
            if (deflt == null) {
                return null;
            }
            else {
                return deflt;
            }
        }
        else if (deflt == null) {
            return new DBConfig(driver, host, port, database);
        }
        else {
            return new DBConfig((driver == null)?deflt.driver:driver,
                    (host == null)?deflt.host:host,
                    (port == null)?deflt.port:port,
                    (database == null)?deflt.database:database,
                    deflt.ssl,
                    deflt.sslfactory,
                    deflt.default_user);
        }
    }

    DBConfig(String driver, String host, Integer port, String database) {
        this(driver, host, port, database, false, null, null);
    }

    DBConfig(String driver, String host, Integer port, String database, boolean ssl, String sslfactory, String default_user) {
        this.driver = driver;
        this.host = host;
        this.port = port;
        this.database = database;
        this.ssl = ssl;
        this.sslfactory = sslfactory;
        this.default_user = default_user;
    }
}
