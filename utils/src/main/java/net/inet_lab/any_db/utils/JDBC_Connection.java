package net.inet_lab.any_db.utils;

import java.util.Properties;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.SQLFeatureNotSupportedException;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JDBC_Connection implements JConnection {
    private static final Logger log = LogManager.getLogger(JDBC_Connection.class);

    static public final String RS_DEFAULT = "DEFAULT";
    static public final String RS_CACHED = "CACHED";
    static public final String RS_WRAPPER = "WRAPPER";

    private Connection conn = null;
    private Statement stmt = null;
    private boolean dry_run;

    private final String jdbcURL;
    private final String jdbcUser;
    private final String jdbcPass;

    private final String jdbcSchema;

    private final boolean supportsCached;
    private final boolean auto_commit;
    private final String rs_type;
    private final String database;

    static {
        // Put the redshift driver at the end so that it doesn't
        // conflict with postgres queries
        // https://stackoverflow.com/questions/31951518/redshift-and-postgres-jdbc-driver-both-intercept-jdbc-postgresql-connection-st
        java.util.Enumeration<Driver> drivers =  DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            Driver d = drivers.nextElement();
            if (d.getClass().getName().startsWith("com.amazon.redshift.")) {
                try {
                    DriverManager.deregisterDriver(d);
                    DriverManager.registerDriver(d);
                } catch (SQLException e) {
                    throw new RuntimeException("Could not deregister redshift driver");
                }
                break;
            }
        }
    }

    public JDBC_Connection(String jdbcSchema,
                           String host, Integer port, String database,
                           String user, String pass,
                           boolean ssl, String sslfactory,
                           Properties options, String rs_type, boolean auto_commit) {

        String x_jdbcURL;
        String jdbcDriver = null;

        switch(jdbcSchema.toLowerCase()) {
            case "redshift" :
                supportsCached = false;
                String url = "jdbc:" + jdbcSchema + "://" + host + ":" + port + "/" + database + "?tcpKeepAlive=true";
                if (ssl) {
                    url += "&ssl=true";
                    if (sslfactory != null)
                        url += "&sslfactory=" + sslfactory;
                }
                x_jdbcURL = url;
                break;
            case "mysql" :
                supportsCached = true;
                // allowPublicKeyRetrieval is to fix this connection error
                // java.sql.SQLNonTransientConnectionException: Public Key Retrieval is not allowed
                // https://stackoverflow.com/questions/50379839/connection-java-mysql-public-key-retrieval-is-not-allowed
                x_jdbcURL = "jdbc:" + jdbcSchema + "://" + host + ":" + port + "/" + database + "?allowPublicKeyRetrieval=true&useSSL=false";
                break;
            case SQLConnector.GCLOUD_MYSQL:
                // https://github.com/GoogleCloudPlatform/cloud-sql-jdbc-socket-factory/blob/main/docs/jdbc.md
                // host is the instance connection name "project:region:instance"; credentials are
                // Google Application Default Credentials, and user is the IAM email truncated at '@'
                supportsCached = true;
                x_jdbcURL = "jdbc:mysql:///" + database +
                        "?socketFactory=com.google.cloud.sql.mysql.SocketFactory" +
                        "&cloudSqlInstance=" + host +
                        "&enableIamAuth=true" +
                        // bill Cloud SQL Admin API calls to the instance's project
                        "&cloudSqlAdminQuotaProject=" + host.split(":")[0] +
                        // socket factory already provides TLS
                        "&sslMode=DISABLED";
                // ignored with IAM auth, but the driver rejects an empty password
                if (pass == null)
                    pass = "iam-auth";
                break;
            case "snowflake":
                supportsCached = false;
                final String default_warehouse = "WHAM_ADHOC";
                final String warehouse = ((options == null)? default_warehouse : options.getProperty("warehouse",
                        options.getProperty("query_group", default_warehouse))).toUpperCase();
                final String role = (options == null)? null: options.getProperty("role");
                x_jdbcURL = "jdbc:" + jdbcSchema + "://" + host + "/?warehouse=" +  warehouse + "&db=" + database;
                if (role != null)
                    x_jdbcURL += "&role=" + role;
                // not sure if this needed, but it seems to help when we can't load Snowflake driver
                jdbcDriver = "net.snowflake.client.api.driver.SnowflakeDriver";
                break;
            case "sqlserver":
                supportsCached = true;
                // mssql-jdbc defaults to encrypt=true (with certificate validation);
                // jTDS, which we used before, never encrypted, so keep that unless ssl is requested
                x_jdbcURL = "jdbc:" + jdbcSchema + "://" + host + ":" + port + ";databaseName=" + database +
                        ";selectMethod=cursor;encrypt=" + ssl;
                break;
            case "trino":
                supportsCached = true;
                // database is "catalog" or "catalog/schema"
                x_jdbcURL = "jdbc:" + jdbcSchema + "://" + host + ":" + port + "/" + database;
                if (ssl)
                    x_jdbcURL += "?SSL=true";
                break;
            default:
                supportsCached = true;
                x_jdbcURL = "jdbc:" + jdbcSchema + "://" + host + ":" + port + "/" + database + "?useSSL=false";
        }

        if (jdbcDriver != null) {
            try {
                Class.forName(jdbcDriver);
            } catch (ClassNotFoundException err) {
                System.err.println(ExceptionUtils.getRootCauseMessage(err));
                System.exit(1);
            }
            System.out.println("Loaded JDBC driver " + jdbcDriver);
        }

        if (pass == null) {
            if ("snowflake".equalsIgnoreCase(jdbcSchema)) {
                x_jdbcURL += "&authenticator=externalbrowser";
                // FIXME: use proper config
                user += "@my_company.com";
            }
        }

        dry_run = false;
        jdbcUser = user;
        jdbcPass = pass;
        jdbcURL = x_jdbcURL;
        this.jdbcSchema = jdbcSchema;
        this.rs_type = rs_type;
        this.auto_commit = auto_commit;
        this.database = database;
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        if ("trino".equalsIgnoreCase(jdbcSchema) && sql.endsWith(";"))
            sql = sql.substring(0,sql.length() - 1);

        if (RS_DEFAULT.equals(rs_type)) {
            initConn(false);
            return stmt.executeQuery(sql);
        }
        else if (RS_CACHED.equals(rs_type)) {
            CachedRowSet crs = RowSetProvider.newFactory().createCachedRowSet();

            crs.setUsername(jdbcUser);
            crs.setPassword(jdbcPass);
            crs.setUrl(jdbcURL);

            crs.setCommand(sql);
            System.out.println("Connecting to: " + jdbcURL + " as user " + jdbcUser + " (using CachedRowSet)");
            crs.execute();

            return crs;
        }
        else if (RS_WRAPPER.equals(rs_type)){
            CachedRowSetWrapper crs = new CachedRowSetWrapper();
            initConn(false);
            crs.setCommand(sql);
            crs.executeWithStatement(stmt);

            return crs;
        }
        else {
            throw new RuntimeException("Unexpected value of rs_type = '" + rs_type + "'");
        }
     }

    public void initConn(boolean force_reset) throws SQLException {
        if (force_reset && conn != null)
            close ();

        if (conn == null) {
            System.out.println("Connecting to: " + jdbcURL + " as user " + jdbcUser);
            conn = DriverManager.getConnection(jdbcURL,jdbcUser,jdbcPass);
            try {
                conn.setAutoCommit(auto_commit);
            }
            catch (SQLFeatureNotSupportedException ignore) {
            }
            stmt = conn.createStatement();
        }
    }

    @Override
    public String getDatabase() {
        return database;
    }

    public int executeUpdate(String sql) throws SQLException {
        initConn(false);
        if ("trino".equalsIgnoreCase(jdbcSchema) && sql.endsWith(";"))
            sql = sql.substring(0,sql.length() - 1);
        return stmt.executeUpdate(sql);
    }

    public void close () throws SQLException {
        System.out.println("Closing connection to " + jdbcURL);
        if (conn != null) {
            stmt.close();
            conn.close();

            conn = null;
        }
    }

    public void commit () throws SQLException {
        System.out.println("Running COMMIT" + "\n");
        if (!dry_run && conn != null)
            conn.commit ();
    }
 }
