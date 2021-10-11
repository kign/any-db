package net.inet_lab.any_db.utils;

import java.io.IOException;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.File;

import java.sql.ResultSet;
import java.sql.SQLException;

import java.rmi.RemoteException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Properties;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

public class SQLConnector implements SQLConnection {
    private static final Logger log = Logger.getLogger(SQLConnector.class.getName());
    static private final String PGPASS = ".pgpass";

    static public final String RS_DEFAULT = JDBC_Connection.RS_DEFAULT;
    static public final String RS_CACHED = JDBC_Connection.RS_CACHED;
    static public final String RS_WRAPPER = JDBC_Connection.RS_WRAPPER;

    static public final String RMI_YES = "YES";
    static public final String RMI_NO = "NO";
    static public final String RMI_DEFAULT = "DEFAULT";

    static public final String SQLPRN_NONE = "NONE";
    static public final String SQLPRN_STDOUT = "STDOUT";
    static public final String SQLPRN_LOG = "LOG";

    private JConnection conn;
    private final boolean dry_run;
    private String md_flavor;
    private final String query_group;
    private final String rmi_server;
    private final String driver;
    private final String signature;
    private final boolean auto_commit = true;

    private final SQLRemote remoteService;

    private static class ConnectionArgs {
        ConnectionArgs(DBConfig dbConfig, String user, String pass, Properties options, String rs_type, int default_port) {
            this.host =  dbConfig.getHost();
            this.port = (dbConfig.getPort() == null)? default_port : dbConfig.getPort();
            this.database = dbConfig.getDatabase();
            ssl = dbConfig.getSsl();
            sslfactory = dbConfig.getSslfactory();
            this.user = user;
            this.pass = pass;
            this.options = options;
            this.rs_type_local = (rs_type == null)?JDBC_Connection.RS_DEFAULT:rs_type;
            this.rs_type_remote = (rs_type == null)?JDBC_Connection.RS_WRAPPER:rs_type;
        }
        public final String host;
        public final int port;
        public final String database;
        final boolean ssl;
        final String sslfactory;
        public final String user;
        public       String pass;
        public final Properties options;
        public final String rs_type_local;
        public final String rs_type_remote;
    }

    private final ConnectionArgs c;

    public static SQLConnection connect(final DBConfig dbConfig, final String user, final String pass, final Properties options,
                                        final boolean dry_run, final String rmi_option, final String rmi_server, final String rs_type)
            throws IOException, SQLException {
        return new SQLConnector(dbConfig, user, pass, options, dry_run, rmi_option, rmi_server, rs_type);
    }

    private SQLConnector(final DBConfig dbConfig, final String user, final String pass, final Properties options,
                         final boolean dry_run, final String rmi_option, final String rmi_server, final String rs_type)
            throws IOException, SQLException {
        driver =  dbConfig.getDriver();

        final int default_port;
        String x_query_group = null;
        switch (driver) {
            case "redshift":
                //jdbcDriver = "com.amazon.redshift.jdbc41.Driver";
                default_port = 5439;
                x_query_group = (options==null)? "default": options.getProperty("query_group", "default");
                break;
            case "mysql":
                //jdbcDriver = "com.mysql.jdbc.Driver";
                default_port = 3306;
                break;
            case "presto":
                //jdbcDriver = "com.facebook.presto.jdbc.PrestoDriver";
                default_port = 8080;
                break;
            case "postgresql":
                //jdbcDriver = "org.postgresql.Driver";
                default_port = 5432;
                break;
            case "snowflake":
                //jdbcDriver = "net.snowflake.client.jdbc.SnowflakeDriver";
                default_port = 0;
                break;
            case "sqlserver":
                default_port = 1433;
                break;
            default:
                default_port = -1;
                log.error("Driver " + driver + " is either misspelled or not supported yet");
                System.exit(1);
        }

        c = new ConnectionArgs (dbConfig, user, pass, options, rs_type, default_port);

        query_group = x_query_group;

        if (c.pass == null) {
            FileInputStream fileInputStream = new FileInputStream(System.getenv().get("HOME") + "/" + PGPASS);
            InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
            BufferedReader fh = new BufferedReader(inputStreamReader);
            String line;
            while((line = fh.readLine()) != null) {
                String[] comps = line.split(":");
                if (comps[0].equals(c.host) &&
                        ("".equals(comps[1])? String.valueOf(default_port): comps[1]).
                                equals(String.valueOf(c.port)) &&
                        comps[2].equals(c.database) &&
                        comps[3].equals(user))
                    c.pass = comps[4];
            }
        }
        if (c.pass == null)
            System.out.println("Password for " + user + "@" + c.host + ":" + c.port + "/" + c.database + " not found in ~/" + PGPASS);


        SQLRemote _remoteService = null;
        if (rmi_server == null || RMI_NO.equals(rmi_option) ||
                (RMI_DEFAULT.equals(rmi_option) && !("snowflake".equals(driver) && c.pass == null)) ) {
            conn = new JDBC_Connection(driver,
                    c.host, c.port, c.database, c.user, c.pass, c.ssl, c.sslfactory, c.options, c.rs_type_local,
                    auto_commit);
        }
        else {
            Registry registry = LocateRegistry.getRegistry(rmi_server, CommonParameters.REGISTRY_PORT);
            System.out.println("OK: Registry located");
            final String SQL_REMOTE = "SQLRemote";

            try {
                _remoteService = (SQLRemote) registry.lookup(SQL_REMOTE);
            } catch (NotBoundException e) {
                System.err.println("Cannot locate remote class " + SQL_REMOTE + ";\n error = " + e);
                System.exit(1);
            } catch (java.rmi.ConnectException e) {
                System.err.println("Cannot locate remote class " + SQL_REMOTE + "; make sure you started the server on port " +
                        CommonParameters.REGISTRY_PORT + "\n  error = " + e);
                System.exit(1);
            }

            System.out.println("OK: Remote class located");
        }
        this.dry_run = dry_run;
        this.rmi_server = rmi_server;
        this.signature = String.format("%s@%s:%s", c.user, c.host, c.database.replace('/', '_').toLowerCase());
        this.remoteService = _remoteService;

        if (this.remoteService != null)
            conn = getRemoteConn (false);
    }

    private JConnection getRemoteConn(boolean reconnect) throws IOException, SQLException {
        JConnection jConnection = null;
        try {
            jConnection = remoteService.connect(driver,
                    c.host, c.port, c.database,
                    c.user, c.pass,
                    c.ssl, c.sslfactory, c.options, c.rs_type_remote, reconnect);
        } catch (java.rmi.ConnectException e) {
            System.err.println("Connection failed, RMI server might not be running");
            System.exit(1);
        }
        System.out.println("OK: Connection acquired");
        return jConnection;
    }

    public void setMarkdownFlavor(String md_flavor) {
        this.md_flavor = md_flavor;
    }

    @Override
    public void consumate() throws SQLException, RemoteException {
        conn.initConn(false);
    }

    @Override
    public String getDriver() {
        return driver;
    }

    @Override
    public String getDatabase() {
        try {
            return conn.getDatabase();
        } catch (RemoteException e) {
            return null;
        }
    }

    @Override
    public String getSignature() {
        return signature;
    }

    @Override
    public void reset() throws SQLException, RemoteException {
        conn.initConn(true);
    }

    public ResultSet select(String sql) throws SQLException, IOException {
        return select(sql, SQLPRN_STDOUT);
    }

    public ResultSet select(String sql, String sqlprn) throws SQLException, IOException {
        print_sql(sql, sqlprn);
        if (dry_run)
            return null;

        for (int attempt = 0; attempt < 2; attempt ++) {
            try {
                ResultSet rs = conn.executeQuery(augumentSql(sql));
                return rs;
            } catch (SQLException err) {
                final String msg = ExceptionUtils.getRootCauseMessage(err);
                if (attempt == 0 && remoteService != null && msg.contains("Authentication token has expired")) {
                    System.out.println(msg);
                    conn = getRemoteConn(true);
                } else
                    throw err;
            }
        }
        return null; // should never get here
    }

    public int execute(String sql) throws SQLException, RemoteException {
        print_sql(sql, SQLPRN_STDOUT);
        if (dry_run) return 0;
        int res = conn.executeUpdate(augumentSql(sql));
        if (!auto_commit)
            commit ();
        return res;
    }

    public void close () throws SQLException, RemoteException {
        if (rmi_server == null)
            conn.close ();
    }

    private void print_sql(String sql, String sqlprn) {
        if (SQLPRN_STDOUT.equals(sqlprn)) {
            if ("github".equals(md_flavor)) {
                System.out.println("```sql");
            } else if ("jira".equals(md_flavor)) {
                System.out.println("{code:sql}");
            }
            String formatted = Misc.beautifySql(sql);
            String hlt_cfg = System.getenv().get("HOME") + "/highlight.json";
            File f = new File(hlt_cfg);
            if (f.exists())
                Misc.pipe_to_stdout(formatted, new String[]{"highlight", "-t", hlt_cfg, "-l", "sql"});
            else
                Misc.pipe_to_stdout(formatted, new String[]{"highlight", "-l", "sql"});
            System.out.println("");
            if ("github".equals(md_flavor)) {
                System.out.println("```");
            } else if ("jira".equals(md_flavor)) {
                System.out.println("{code}");
            }
        }
        else if (SQLPRN_LOG.equals(sqlprn)) {
            log.info("Executing: " + sql);
        }
        else if (SQLPRN_NONE.equals(sqlprn))
            ;
        else
            throw new RuntimeException("Invalid value of sqlprn = " + sqlprn);
    }

    private String augumentSql(String sql) {
        if (query_group != null)
            sql = "set query_group='" + query_group + "'; " + sql;

        return sql;
    }

    private void commit () throws SQLException, RemoteException {
        if (!dry_run)
            conn.commit ();
    }
}
