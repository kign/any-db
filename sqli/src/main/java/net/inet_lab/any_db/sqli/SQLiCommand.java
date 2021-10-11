package net.inet_lab.any_db.sqli;

import java.util.Collection;
import java.util.Map;
import java.util.Arrays;

import java.io.IOException;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Pattern;

// import net.inet_lab.any_db.utils.*;
import net.inet_lab.any_db.utils.SQLConnection;
import net.inet_lab.any_db.utils.FormattedOutput;
import net.inet_lab.any_db.utils.SQLConnector;
import net.inet_lab.any_db.utils.TransposedListing;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import org.jline.terminal.Terminal;

import static net.inet_lab.any_db.utils.Misc.*;

public class SQLiCommand {
    private static final Logger log = Logger.getLogger(SQLi.class.getName());
    private final SQLConnection conn;
    private final SQLDict dict;
    private final SQLiContext ctx;
    private final Terminal term;
    private final PrintStream ps;
    private final boolean printStackTrace;
    
    SQLiCommand (SQLConnection conn, SQLDict dict, SQLiContext ctx, Terminal term, PrintStream ps) {
        this.conn = conn;
        this.dict = dict;
        this.ctx = ctx;
        this.term = term;
        this.ps = ps;

        printStackTrace = false;
    }

    enum Command {
        SHOW_SCHEMAS("List all schemas in the database", "", "s"),
        SHOW_TABLES("List all tables and views in schema", "[schema]", "dt"),
        SHOW_TABLE("List all columns in a table", "<table or view>", "d"),
        DEFINE("Print full SQL definition of table or view", "<table or view>", "def"),
        COLUMNS("Print all colums in transposed format", "<table> <filter>", "col", "columns"),
        TESTARGS("This is to test argument parsing", "<arg1> .... <argN>", "echo"),
        EXIT("Exit the session", "", "exit", "quit"),
        SET("Define parameter (print all pars if no args)", "<par> [value]", "set"),
        UNSET("Undefine parameter", "par", "unset", "undef"),
        PARS("Enable or disable parameter substitution", "<enable|disable>", "pars"),
        ADMIN("Switch to admin mode", "", "admin"),
        RESET("Reset JDBC connection", "", "reset"),
        HELP("Print help", "", "h", "help");

        final String[] args;
        final String opts;
        final String help;
        Command(String help, String opts, String... args) {
            this.args = args;
            this.opts = opts;
            this.help = help;
        }
    }

    void execute(String cmd) throws IOException {
        execute(cmd, null);
    }

    void execute (String cmd, Integer termWidth) throws IOException {
        // String[] args = cmd.split("\\s+");
        String[] args = splitArgs(cmd);

        Command oper = null;
        for (Command c : Command.values()) {
            for (String arg : c.args) {
                if (("\\" + arg).equalsIgnoreCase(args[0])) {
                    if (oper != null) {
                        System.err.println("Command " + args[0] + " is in conflict between " + oper + " and " + c + " !");
                    }
                    oper = c;
                }
            }
        }

        if (oper == null) {
            System.err.println("Command '" + args[0] + "' is not supported");
            return;
        }

        String[] args1 = new String[args.length - 1];
        System.arraycopy(args,1, args1,0, args.length - 1);

        try {
            execute_low(oper, args1, termWidth);
        } catch (SQLException err) {
            System.err.println(ExceptionUtils.getRootCauseMessage(err));
            if (printStackTrace)
                err.printStackTrace();
        }
        catch (SQLDict.Error err) {
            System.err.println("SQLDict error: " + err);
            if (printStackTrace)
                err.printStackTrace();
        }
    }

    private void execute_low (Command cmd, String[] args, Integer termWidth) throws IOException, SQLException, SQLDict.Error {
        String driver = conn.getDriver();
        if (cmd == Command.SHOW_SCHEMAS) {
            if (args.length > 0) {
                System.err.println("Command '" + cmd + "' takes no arguments");
                return;
            }

            ps.println("All schemas");
            Collection<SQLDict.Schema> schemas = dict.getSchemas(false);
            FormattedOutput out = getOutput();
            out.addColumn("name", 30, true);
            out.addColumn("owner", 30, true);

            for (SQLDict.Schema schema : schemas) {
                out.writerow(schema.name, (schema.owner==null)?"":schema.owner);
            }
            out.close();
        }
        else if (cmd == Command.SHOW_TABLES) {
            if (args.length > 1) {
                System.err.println("Command '" + cmd + "' takes no more than one argument");
                return;
            }
            String schema = getArg(args, 0, ctx.getSchema());
            ps.println("All tables in schema " + schema);
            Collection<SQLDict.Table> tables = dict.getTables(schema, false);
            FormattedOutput out = getOutput();
            out.addColumn("name", 50, true);
            out.addColumn("owner", 30, true);
            out.addColumn("kind", 30, true);
            out.addColumn("size", 30, true);

            for (SQLDict.Table table : tables) {
                out.writerow(table.name,
                             coalesce(table.owner, ""),
                             table.type.toString(),
                             coalesce(sizePresentation(table.size), ""));
            }
            out.close();
        }
        else if (cmd == Command.SHOW_TABLE) {
            if (args.length != 1) {
                System.err.println("Command '" + cmd + "' expects exactly one argument");
                return;
            }
            String tname = args[0];
            ps.println("Viewing table " + tname);
            Collection<SQLDict.Column> cols = dict.getColumns(tname, ctx.getSchema(), false);
            FormattedOutput out = getOutput();
            out.addColumn("Column", 60, true);
            out.addColumn("Type", 30, true);
            out.addColumn("Extra", 30, true);

            for (SQLDict.Column col : cols) {
                out.writerow(col.name,
                             col.type,
                             join(", ", col.nullable? null: "not null",
                                     (col.more== null || col.more.equals(""))? null: col.more));
            }
            ctx.setLastCheckedTable(tname);
            out.close();
        }
        else if (cmd == Command.DEFINE) {
            if (args.length != 1) {
                System.err.println("Command '" + cmd + "' expects exactly one argument");
                return;
            }
            String tname = args[0];
            int ii = tname.lastIndexOf('.');
            String tschema, tbl;
            if (ii == -1) {
                tschema = ctx.getSchema();
                tbl = tname;
            }
            else {
                tschema = tname.substring(0, ii);
                tbl = tname.substring(ii + 1);
            }
            String def = null;
            if ("snowflake".equalsIgnoreCase(driver)) {
                ResultSet res = conn.select("select get_ddl('table','" + tname + "')");
                res.next();
                def = res.getString(1);
                if (def.startsWith("create or replace TABLE"))
                    def = dict.globalColNameReplaceSnowflake(def.toLowerCase());
            }
            else if ("sqlserver".equalsIgnoreCase(driver)) {
                ResultSet res = conn.select(String.format("select object_definition(object_id('%s.%s'))", tschema, tbl));
                res.next();
                def = res.getString(1);
                if (def == null)
                    def = "Insufficient permissions";
            }
            else if ("redshift"   .equals(driver)   ||
                    "postgresql" .equals(driver) ) {
                ResultSet res = conn.select(
                        String.format("select definition from pg_catalog.pg_views where schemaname='%s' and viewname='%s';",
                                 tschema, tbl),
                        SQLConnector.SQLPRN_LOG);
                while(res.next())
                    def = res.getString(1);
                if (def != null)
                    def = beautifySql(def);
                else {
                    Collection<SQLDict.Column> cols = dict.getColumns(tbl, tschema, false);
                    ii = 0;
                    StringBuilder sb = new StringBuilder();
                    sb.append("create table " + tschema + "." + tbl + " (\n");
                    for (SQLDict.Column col : cols) {
                        ii ++;
                        sb.append(String.format("  %-30s %-20s %s%s\n", col.name, col.type, col.nullable?"":"not null", (ii < cols.size())?",":")"));
                    }
                    def = sb.toString();
                }
            }
            else if ("presto".equals(driver) ||
                    "mysql".equals(driver)) {
                ResultSet res = conn.select("show create table " + tname, SQLConnector.SQLPRN_LOG);
                Map<String, Integer> idx = SQLDict.getIndeces(res, "create*");
                if (res.next())
                    def = res.getString(idx.get("create*"));
                else
                    System.err.println("Bad table or other error");
            }
            else {
                System.err.println("Command '" + cmd + "' not implemented for driver " + driver);
                return;
            }
            ctx.setLastCheckedTable(tname);
            ps.println(def);
        }
        else if (cmd == Command.HELP) {
            if (args.length > 0) {
                System.err.println("Command '" + cmd + "' takes no arguments");
                return;
            }

            for (Command x : Command.values()) {
                StringBuilder y = new StringBuilder ();
                for (String arg : x.args) {
                    if (y.length() > 0)
                        y.append(", ");
                    y.append("\\" + arg);
                }

                System.out.println(String.format("%-20s %-20s %s", y, x.opts, x.help));
            }
        }
        else if (cmd == Command.SET) {
            if (args.length == 0 && !ctx.getParamEnabled())
                ps.println("Parameters substitution disabled, to enable, type: \\pars enable");
            else if (args.length == 0)
                print_parameters ();
            else {
                String var = args[0];
                String val = getArg(args,1, "");
                ctx.setVariable(var, val);
                ctx.setParamEnabled(true);
            }
        }
        else if (cmd == Command.UNSET) {
            if (args.length != 1) {
                System.err.println("Command '" + cmd + "' expects exactly one argument");
                return;
            }

            ctx.delVariable(args[0]);
        }
        else if (cmd == Command.PARS) {
            if (args.length == 0) {
                ps.println(ctx.getParamEnabled()?"enabled":"disabled");
            }
            else if (args.length > 1)
                System.err.println("Command '" + cmd + "' expects no more than one argument");
            else if ("enable".equalsIgnoreCase(args[0])) {
                print_parameters();
                ctx.setParamEnabled(true);
            }
            else if ("disable".equalsIgnoreCase(args[0])) {
                ps.println("disabled");
                ctx.setParamEnabled(false);
            }
            else
                System.err.println("Invalid argument '" + args[0] + "', expecting 'enable' or 'disable'");
        }
        else if (cmd == Command.ADMIN) {
            if ("snowflake".equalsIgnoreCase(driver)) {
                conn.execute("use role sysadmin");
                ps.println("OK");
            }
            else
                System.err.println("Command " + cmd + " only supported in SNowflake");
        }
        else if (cmd == Command.COLUMNS) {
            int limit = 0;
            int N = args.length;
            if (N >= 3 && args[N - 2].equalsIgnoreCase("limit")) {
                limit = Integer.parseInt(args[N - 1]);
                N -= 2;
            }
            if (N == 0) {
                System.err.println("Usage: \\" + cmd.args[0] + " <table> [filter] [limit]");
                return;
            }
            String filter = "";
            String keyColName = null;
            int delim;

            Pattern pNonAlpha = Pattern.compile("[^a-zA-Z0-9]");

            if (N == 1)
                delim = 2;
            else if (N > 2 && !pNonAlpha.matcher(args[1]).find()) {
                keyColName = args[1];
                filter = "where " + keyColName + " in (" + join(",", Arrays.copyOfRange(args,2,N)) + ")";
                delim = N - 1;
            }
            else {
                filter = "where " + join(" ", Arrays.copyOfRange(args,1, N));
                delim = 10;
            }

            if (limit == 0)
                limit = delim;
            String q = "select * from " + args[0] + " " + filter + " limit " + (1 + limit);
            final int minWidth = 30;
            final int colWidth = 0;
            ResultSet res = conn.select(q);
            TransposedListing.run(res,"NULL", keyColName, limit, termWidth, minWidth, colWidth, null);
        }
        else if (cmd == Command.TESTARGS) {
            for (int ii=0; ii < args.length; ii ++)
                System.out.println("arg[" + (1 + ii) + "] = <" + args[ii] + ">");
        }
        else if (cmd == Command.RESET) {
            conn.reset();
        }
        else
            System.err.println("Command '" + cmd + "' is not yet implemented");
    }

    private FormattedOutput getOutput() throws IOException {
        return FormattedOutput.newTabularOutput (ps, term.getWidth(), 30, null);
    }

    private void print_parameters() throws IOException {
        FormattedOutput out = getOutput();
        out.addColumn("Variable", 30, true);
        out.addColumn("Value", 30, true);

        for (String var : ctx.getAllVariables())
            out.writerow(var, ctx.getVariable(var));
        out.close ();
    }

    static private String sizePresentation(Long bytes) {
        if (bytes == null)
            return null;
        if (bytes >= 2 * 0x10000000000L)
            return String.format("%.2fT", 1.0 * bytes / 0x10000000000L);
        if (bytes >= 2 * 0x40000000L)
            return String.format("%.2fG", 1.0 * bytes / 0x40000000L);
        if (bytes >= 2 * 0x100000L)
            return String.format("%.2fM", 1.0 * bytes / 0x100000L);
        if (bytes >= 2 * 0x400L)
            return String.format("%.2fK", 1.0 * bytes / 0x400L);
        return String.valueOf(bytes) + "b";
    }

}
