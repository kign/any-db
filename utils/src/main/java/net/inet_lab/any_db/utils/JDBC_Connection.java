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
import com.sun.rowset.CachedRowSetImpl;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

public class JDBC_Connection implements JConnection {
    private static final Logger log = Logger.getLogger(JDBC_Connection.class.getName());

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
            if (d.getClass().getName().equals("com.amazon.redshift.jdbc41.Driver")) {
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
                jdbcDriver = "net.snowflake.client.jdbc.SnowflakeDriver";
                break;
            case "sqlserver":
                supportsCached = true;
                x_jdbcURL = "jdbc:" + "jtds:" + jdbcSchema + "://" + host + ":" + port + "/" + database + ";selectMethod=cursor";
                jdbcDriver = "net.sourceforge.jtds.jdbc.Driver";
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
        if ("presto".equalsIgnoreCase(jdbcSchema) && sql.endsWith(";"))
            sql = sql.substring(0,sql.length() - 1);

        if (RS_DEFAULT.equals(rs_type)) {
            initConn(false);
            return stmt.executeQuery(sql);
        }
        else if (RS_CACHED.equals(rs_type)) {
            CachedRowSet crs = new CachedRowSetImpl();

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
        if ("presto".equalsIgnoreCase(jdbcSchema) && sql.endsWith(";"))
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
