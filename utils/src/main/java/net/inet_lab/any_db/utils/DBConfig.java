package net.inet_lab.any_db.utils;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.YAMLException;

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

    static private final String CONFIG_FILE = ".any-db.yaml";
    static private final String LEGACY_CONFIG_FILE = ".any-db";
    static private final List<String> CONNECTION_KEYS =
            List.of("driver", "host", "port", "database", "user", "ssl", "sslfactory", "alias", "enabled");
    static private final Set<String> TRUE_VALUES = Set.of("true", "yes", "on", "y", "1");
    static private final Set<String> FALSE_VALUES = Set.of("false", "no", "off", "n", "0");

    // LinkedHashMap: connections are listed in the order they appear in the config file
    static final private Map<String,DBConfig> knownConfigs = new LinkedHashMap<>();

    static {
        // built-in servers
        knownConfigs.put("name", new DBConfig ("redshift", "XXX.redshift.amazonaws.com", 5439, "redshift_db"));

        Path home = Paths.get(System.getProperty("user.home"));
        Path configFile = home.resolve(CONFIG_FILE);
        if (Files.exists(configFile))
            loadConfigFile(configFile);
        else if (Files.exists(home.resolve(LEGACY_CONFIG_FILE)))
            System.err.println("Warning: ~/" + LEGACY_CONFIG_FILE + " is no longer used; move connections to ~/" + CONFIG_FILE);
    }

    /*
     * Expected format:
     *
     * connections:
     *   pro:
     *     driver: mysql          # optional, default = postgresql
     *     host: 111.111.111.111  # required
     *     port: 3306             # optional, default depends on driver
     *     database: clients      # required
     *     user: john             # optional
     *     ssl: true              # optional, default = false
     *     sslfactory: ...        # optional
     *     alias: pr, pro2        # optional, other names for the same connection (comma-separated or a list)
     *     enabled: false         # optional, default = true; disabled connections (and their aliases) are ignored
     */
    private static void loadConfigFile(Path configFile) {
        Object root;
        try (Reader in = Files.newBufferedReader(configFile)) {
            LoaderOptions options = new LoaderOptions();
            options.setAllowDuplicateKeys(false);
            root = new Yaml(new SafeConstructor(options)).load(in);
        }
        catch (IOException | YAMLException e) {
            throw new RuntimeException("Error reading " + configFile + ": " + e.getMessage(), e);
        }

        if (root == null)   // empty file
            return;
        if (!(root instanceof Map))
            throw configError(configFile, null, "top level must be a mapping with key \"connections\"");
        for (Object key : ((Map<?,?>) root).keySet())
            if (!"connections".equals(key))
                throw configError(configFile, null, "unknown top-level key \"" + key + "\", expected \"connections\"");

        Object connections = ((Map<?,?>) root).get("connections");
        if (connections == null)
            return;
        if (!(connections instanceof Map))
            throw configError(configFile, null, "\"connections\" must be a mapping of connection names to their properties");

        // name or alias -> connection that defined it, to detect conflicts
        Map<String,String> definedBy = new HashMap<>();

        for (Map.Entry<?,?> entry : ((Map<?,?>) connections).entrySet()) {
            String name = String.valueOf(entry.getKey());
            if (!(entry.getValue() instanceof Map))
                throw configError(configFile, name, "must be a mapping of properties " + CONNECTION_KEYS);
            Map<?,?> props = (Map<?,?>) entry.getValue();

            if (!getBoolean(configFile, name, props, "enabled", true))
                continue;

            for (Object key : props.keySet())
                if (!CONNECTION_KEYS.contains(key))
                    throw configError(configFile, name, "unknown property \"" + key + "\", valid properties are " + CONNECTION_KEYS);

            String driver = getString(configFile, name, props, "driver");
            String host = getString(configFile, name, props, "host");
            String database = getString(configFile, name, props, "database");
            String user = getString(configFile, name, props, "user");
            String sslfactory = getString(configFile, name, props, "sslfactory");
            if (host == null)
                throw configError(configFile, name, "host must be defined");
            if (database == null)
                throw configError(configFile, name, "database must be defined");

            Object port = props.get("port");
            if (port != null && !(port instanceof Integer))
                throw configError(configFile, name, "port must be an integer");

            boolean ssl = getBoolean(configFile, name, props, "ssl", false);

            DBConfig config = new DBConfig((driver == null)? "postgresql" : driver,
                    host, (Integer) port, database, ssl, sslfactory, user);

            List<String> names = new ArrayList<>();
            names.add(name);
            names.addAll(getAliases(configFile, name, props));
            for (String n : names) {
                String other = definedBy.putIfAbsent(n, name);
                if (other != null)
                    throw configError(configFile, name, "\"" + n + "\" is already defined " +
                            (other.equals(n)? "as a connection" : "as an alias of connection \"" + other + "\""));
                knownConfigs.put(n, config);
            }
        }
    }

    private static List<String> getAliases(Path configFile, String name, Map<?,?> props) {
        Object value = props.get("alias");
        List<?> items;
        if (value == null)
            return List.of();
        else if (value instanceof String)
            items = Arrays.asList(((String) value).split(","));
        else if (value instanceof List)
            items = (List<?>) value;
        else
            throw configError(configFile, name, "alias must be a comma-separated string or a list of names");

        List<String> aliases = new ArrayList<>();
        for (Object item : items) {
            String alias = (item == null)? "" : String.valueOf(item).trim();
            if (alias.isEmpty())
                throw configError(configFile, name, "alias contains an empty name");
            aliases.add(alias);
        }
        return aliases;
    }

    // Unquoted yes/no/on/off are already booleans in YAML 1.1; also accept quoted values and y/n, 1/0
    private static boolean getBoolean(Path configFile, String name, Map<?,?> props, String key, boolean deflt) {
        Object value = props.get(key);
        if (value == null)
            return deflt;
        if (value instanceof Boolean)
            return (Boolean) value;
        String s = String.valueOf(value).trim().toLowerCase();
        if (TRUE_VALUES.contains(s))
            return true;
        if (FALSE_VALUES.contains(s))
            return false;
        throw configError(configFile, name, key + " must be true or false (or yes/no, on/off)");
    }

    private static String getString(Path configFile, String name, Map<?,?> props, String key) {
        Object value = props.get(key);
        if (value == null || value instanceof String)
            return (String) value;
        // e.g. 'host: 1.2' is parsed as a number and 'database: no' as a boolean
        throw configError(configFile, name, key + " must be a string (put value in quotes)");
    }

    private static RuntimeException configError(Path configFile, String name, String message) {
        return new RuntimeException("Error in file " + configFile +
                ((name == null)? "" : ", connection \"" + name + "\"") + ": " + message);
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
        // "presto" is a legacy name; we now use the Trino JDBC driver
        this.driver = "presto".equalsIgnoreCase(driver) ? "trino" : driver;
        this.host = host;
        this.port = port;
        this.database = database;
        this.ssl = ssl;
        this.sslfactory = sslfactory;
        this.default_user = default_user;
    }
}
