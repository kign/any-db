package net.inet_lab.any_db.sqli;

import java.io.File;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.util.List;
import java.util.ArrayList;

import java.io.IOException;

import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.lang3.exception.ExceptionUtils;

import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.TerminalBuilder;
import org.jline.terminal.Terminal;

import net.inet_lab.any_db.utils.SQLConnector;
import net.inet_lab.any_db.utils.SQLConnection;
import net.inet_lab.any_db.utils.CFormatter;
import net.inet_lab.any_db.utils.DBConfig;
import net.inet_lab.any_db.utils.DBUserConfig;
import net.inet_lab.any_db.utils.ParsedArgsBaseDBAccess;
import net.inet_lab.any_db.utils.FormattedOutput;
import static net.inet_lab.any_db.utils.Misc.*;


class SQLi {
    private static final Logger log = Logger.getLogger(SQLi.class.getName());

    static class ParsedArgs extends ParsedArgsBaseDBAccess {
        final String loginUser = System.getenv().get("USER");

        final DBConfig dbConfig;
        final String user;

        final String query_group;
        final boolean is_admin;

        final String nullString = "NULL";

        final int minWidth = 30;
        final boolean timingEnabled = true;

        ParsedArgs(String[] commandLine) {
            StringBuilder selector = new StringBuilder();
            for (String x : DBConfig.values()) {
                if (selector.length() > 0)
                    selector.append('|');
                selector.append(x);
            }
            final String USAGE = "sqli.sh [options] [" + selector + "]";

            HelpFormatter helper = new HelpFormatter();

            // https://commons.apache.org/proper/commons-cli/apidocs/index.html
            Options options = (new Options())
                    .addOption(Option.builder()
                            .longOpt("help")
                            .desc("Print this help")
                            .build())
                    .addOption(Option.builder("h")
                            .hasArg().argName("HOST")
                            .longOpt("host")
                            .desc("host name")
                            .build())
                    .addOption(Option.builder("p")
                            .hasArg().argName("PORT")
                            .longOpt("port")
                            .desc("port number")
                            .build())
                    .addOption(Option.builder("d")
                            .hasArg().argName("database")
                            .longOpt("database")
                            .desc("name of database")
                            .build())
                    .addOption(Option.builder("U")
                            .hasArg().argName("USER")
                            .longOpt("user")
                            .desc("user name (default = current user, " + loginUser + ")")
                            .build())
                    .addOption(Option.builder()
                            .hasArg().argName("DRIVER")
                            .longOpt("driver")
                            .desc("JDBC driver to use")
                            .build())
                    .addOption(Option.builder("q")
                            .hasArg().argName("QUERY_GROUP")
                            .longOpt("query_group")
                            .desc("Query group")
                            .build())
                    .addOption(Option.builder("M")
                            .longOpt("admin")
                            .desc("Assume sysadmin role (snowflake)")
                            .build())
                    .addOption(Option.builder()
                            .hasArg().argName("CONFIG")
                            .longOpt("default-config")
                            .desc("Default config if one not specified as argument")
                            .build());

            parsedCL = parseCL(options, commandLine);

            if (parsedCL.hasOption("help")) {
                helper.printHelp(getTermWidth(), USAGE, "options:", options, "", false);
                System.exit(0);
            }

            final List<String> loArgs = new ArrayList<>();
            dbConfig = getConfig(loArgs, null);

            if (!verifyConfig(dbConfig)) {
                System.exit(1);
            }

            if (loArgs.size() > 0) {
                helper.printHelp(getTermWidth(), USAGE, "options:", options, "Error: Too many arguments", false);
                System.exit(1);
            }

            is_admin = parsedCL.hasOption("admin");
            if (is_admin && !"snowflake".equalsIgnoreCase(dbConfig.getDriver()))
                log.warn("--admin/-M only effective for snowflake, not " + dbConfig.getDriver());

            query_group = optStringVal("query_group");
            user = optStringVal("user", (dbConfig.default_user == null) ? loginUser : dbConfig.default_user);
        }
    }

    public static void main(String[] commandLine)  {

        ParsedArgs args = new ParsedArgs(commandLine);
        Properties options = new Properties();
        if (args.query_group != null)
            options.put("query_group", args.query_group);
        if ("snowflake".equalsIgnoreCase(args.dbConfig.getDriver())) {
            String role = DBUserConfig.getSnowflakeRole(args.is_admin, args.user);
            if (role != null)
                options.put("role", role);
        }

        SQLConnection conn = null;
        try {
            conn = SQLConnector.connect(args.dbConfig, args.user, null, options,
                    false, null, null, null);
            conn.consumate();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        SQLDict dict = new SQLDict(conn);
        SQLiContext ctx = new SQLiContext(conn);

        //System.setProperty("jna.nosys", "true"); // I guess to make it possible for jline to load some native libs
        Terminal t = null;
        try {
            t = TerminalBuilder.builder()
                                    .system(true)
                                    .name("SQLi")
                                    .build();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //t.init();
        //ConsoleReader reader = new ConsoleReader("SQLi", System.in, System.out, t);
        LineReader reader = LineReaderBuilder.builder()
                .terminal(t)
                .completer(new SQLCompleter(conn, dict, ctx))
                .parser(new SQLiParser())
                .build();
        //reader.setPrompt(args.dbConfig.getDatabase().toLowerCase() + "> ");
        String line;
        //reader.addCompleter(new SQLCompleter(conn, dict, ctx));
        String dot_sqli = System.getProperty("user.home") + "/.sqli3";
        if ((new File(dot_sqli)).mkdirs())
            log.info("Created directory " + dot_sqli);
        String historyFile = dot_sqli + "/" + conn.getSignature();
        reader.setVariable(LineReader.HISTORY_FILE, Paths.get(historyFile));
        reader.setOpt(LineReader.Option.HISTORY_INCREMENTAL);
        reader.setOpt(LineReader.Option.HISTORY_TIMESTAMPED);

        //FileHistory h = new FileHistory(new File(historyFile));
        //reader.setHistory(h);
        //reader.setHistoryEnabled(true);
        SQLiCommand cmdExec = new SQLiCommand(conn, dict, ctx, t, System.out);
        log.info("Saving history to " + historyFile);

        while (true) {
            try {
                line = reader.readLine(args.dbConfig.getDatabase().toLowerCase() + "> ");
            }
            catch (EndOfFileException e) {
                break;
            }
            catch(UserInterruptException e) {
                continue;
            }

            String sqlCmdLine = line.trim();
            long t0 = System.currentTimeMillis();

            try {
                if (sqlCmdLine.equalsIgnoreCase("\\quit") || sqlCmdLine.equalsIgnoreCase("\\exit")) {
                    break;
                } else if ("".equals(sqlCmdLine))
                    ;
                else if (sqlCmdLine.charAt(0) == '#')
                    ;
                else if (sqlCmdLine.charAt(0) == '\\')
                    cmdExec.execute(sqlCmdLine, t.getWidth());
                else {
                    Pattern p = Pattern.compile("([a-z_][a-z0-9_]*)\\s*=\\s*(.*)$", Pattern.CASE_INSENSITIVE);
                    Matcher m = p.matcher(sqlCmdLine);
                    if (m.lookingAt())
                        cmdExec.execute("\\set " + m.group(1) + " " + m.group(2));
                    else
                        executeSQL(args, ctx, t.getWidth(), conn, sqlCmdLine);
                }
            }
            catch (Throwable e) {
                e.printStackTrace();
            }
            long t1 = System.currentTimeMillis();

            if (args.timingEnabled && t1 - t0 >= 10)
                System.out.println(String.format("Execution took %.3f secs", (t1 - t0) / 1000.0));
        }
        try {
            conn.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        //h.flush();
    }

    private static void executeSQL(ParsedArgs args, final SQLiContext ctx, int width, SQLConnection conn, String sql) throws IOException, SQLException {
        if (ctx.getParamEnabled()) {
            try {
                sql = substituteParameters(sql, new ParametersAccess() {
                    @Override
                    public String get(String par) {
                        return ctx.getVariable(par);
                    }
                });
            } catch (ParametersError err) {
                System.err.println("ERROR: " + err);
                return;
            }
        }

        Pattern pSelect = Pattern.compile("(select|with|show|desc)\\s", Pattern.CASE_INSENSITIVE);
        Matcher m = pSelect.matcher(sql);
        boolean isUpdate = !m.lookingAt();
        long t0 = System.currentTimeMillis(), t1 = 0;

        ResultSet res = null;
        try {
            if (isUpdate) {
                int nRes = conn.execute(sql);
                double tx = (System.currentTimeMillis() - t0) / 1000.0;
                System.out.println(String.format("Result: %d", nRes));
                log.info(String.format("execute() returned %d; took %.3f secs", nRes, tx));
            }
            else {
                res = conn.select(sql);
                t1 = System.currentTimeMillis();
            }
        }
        catch (Exception err) {
            System.err.println(ExceptionUtils.getRootCauseMessage(err));
            return;
        }

        if (isUpdate)
            return;

        ResultSetMetaData rsmd = res.getMetaData();
        int nCols = rsmd.getColumnCount();
        List<CFormatter> formatters = new ArrayList<>();

        FormattedOutput out;
        out = FormattedOutput.newTabularOutput (System.out, width, args.minWidth, null);

        for (int iCol = 1; iCol <= nCols; iCol ++) {
            CFormatter cf = CFormatter.create(rsmd, iCol, args.nullString);
            String label = rsmd.getColumnLabel(iCol).toLowerCase();
            int idx = label.indexOf('.');
            if (idx < label.length() - 2)
                label = label.substring(idx + 1,label.length());
            out.addColumn(label, 0, cf.getLeftAlign(), cf.getFormatPattern(), null);
            formatters.add(cf);
        }
        int nRows = 0;
        while(res.next()) {
            nRows ++;
             List<String> row = new ArrayList<>();
            for (CFormatter cf : formatters)
                row.add(cf.format(res));
            out.writerow(row);
        }
        final long t2 = System.currentTimeMillis();
        log.info(String.format("Received %d rows, took %.3f + %.3f secs",
                nRows,
                (t1 - t0)/1000.0,
                (t2 - t1)/1000.0));
        out.close ();
     }
}