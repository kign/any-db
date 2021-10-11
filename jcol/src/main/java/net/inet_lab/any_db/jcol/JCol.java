package net.inet_lab.any_db.jcol;

import java.util.List;
import java.util.ArrayList;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Properties;

import net.inet_lab.any_db.utils.*;
import org.apache.log4j.Logger;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.lang3.exception.ExceptionUtils;

public class JCol {
    private static final String JDBC_REDSHIFT_DRIVER = "com.amazon.redshift.jdbc41.Driver";
    private static final Logger log = Logger.getLogger(JCol.class.getName());

    static class ParsedArgs extends ParsedArgsBaseDBAccess {
        final String loginUser = System.getenv().get("USER");
        private final int defaultLimit = 10;

        final DBConfig dbConfig;
        final String user;

        final String filter;

        final String nullString;
        final String outputFile;
        final String tableName;
        final String keyColName;
        final String query_group;
        final boolean is_admin;
        final boolean dry_run = false;
        final String rmi_option;
        final String rmi_server = "localhost";
        final String rs_type = null;

        final int minWidth = 30;
        final int limit;
        final int colWidth;

        ParsedArgs(String[] commandLine) throws IOException {
            StringBuilder selector = new StringBuilder();
            for (String x : DBConfig.values()) {
                if (selector.length() > 0)
                    selector.append('|');
                selector.append(x);
            }
            final String USAGE="jcol.sh [options] [" + selector + "] <table> [<filter|column_name val1 val2 val3.... valN>]";

            HelpFormatter helper = new HelpFormatter();
            String defaultDriver = "redshift";

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
                            .desc("user name (default = " + loginUser + "), could be overriden by config")
                            .build())
                    .addOption(Option.builder()
                            .hasArg().argName("DRIVER")
                            .longOpt("driver")
                            .desc("JDBC driver to use (default = " + defaultDriver + ")")
                            .build())
                    .addOption(Option.builder("O")
                            .hasArg().argName("FILE")
                            .longOpt("output")
                            .desc("Output file name")
                            .build())
                    .addOption(Option.builder()
                            .hasArg().argName("NULL VALUE")
                            .longOpt("null")
                            .desc("NULL value (default = NULL)")
                            .build())
                    .addOption(Option.builder("l")
                            .hasArg().argName("LIMIT")
                            .longOpt("limit")
                            .desc("max number of rows to use (default = " + defaultLimit + ")")
                            .build())
                    .addOption(Option.builder("q")
                            .hasArg().argName("QUERY_GROUP")
                            .longOpt("query_group")
                            .desc("Query group")
                            .build())
                    .addOption(Option.builder("w")
                            .hasArg().argName("WIDTH")
                            .longOpt("width")
                            .desc("max column width (by default filling avaible terminal width)")
                            .build())
                    .addOption(Option.builder("M")
                            .longOpt("admin")
                            .desc("Assume sysadmin role (snowflake)")
                            .build())
                    .addOption(Option.builder()
                            .hasArg().argName("CONFIG")
                            .longOpt("default-config")
                            .desc("Default config if one not specified as argument")
                            .build())
                    .addOption(Option.builder()
                            .longOpt("rmi")
                            .desc("Force RMI use (always execute in server process)")
                            .build())
                    .addOption(Option.builder()
                            .longOpt("no-rmi")
                            .desc("Inhibit RMI use (never execute in server process)")
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

            user = optStringVal("user", (dbConfig.default_user == null)?loginUser:dbConfig.default_user);

            int dlim = defaultLimit;
            if (loArgs.size() < 1) {
                helper.printHelp(getTermWidth(), USAGE, "options:", options, "Error: Missing argument (at least table name must be specified)", false);
                System.exit(1);
                throw new RuntimeException("fake");
            }
            else if (loArgs.size() == 1) {
                tableName = loArgs.get(0);
                filter = null;
                keyColName = null;
                dlim = 1;
            }
            else if (loArgs.size() == 2) {
                tableName = loArgs.get(0);
                filter = loArgs.get(1);
                keyColName = null;
                dlim = 2;
            }
            else {
                tableName = loArgs.get(0);
                StringBuilder bFilter = new StringBuilder();
                keyColName = loArgs.get(1);
                bFilter.append(keyColName).append(" IN (");
                boolean pInt = true;
                for (int ii = 2; ii < loArgs.size(); ii ++) {
                    if (pInt) {
                        try {
                            Integer.parseInt(loArgs.get(ii));
                        }
                        catch(IllegalArgumentException err) {
                            pInt = false;
                        }
                    }
                }

                for (int ii = 2; ii < loArgs.size(); ii ++) {
                    if (!pInt)
                        bFilter.append("'");
                    bFilter.append(loArgs.get(ii));
                    if (!pInt)
                        bFilter.append("'");

                    if (ii < loArgs.size() - 1)
                        bFilter.append(", ");
                }

                bFilter.append(")");
                filter = bFilter.toString();
            }

            is_admin = parsedCL.hasOption("admin");
            if (is_admin && !"snowflake".equalsIgnoreCase(dbConfig.getDriver()))
                log.warn("--admin/-M only effective for snowflake, not " + dbConfig.getDriver());

            nullString = parsedCL.getOptionValue("null", "NULL");

            outputFile = parsedCL.getOptionValue("output");
            limit = optIntegerVal("limit", dlim);
            colWidth = optIntegerVal("width", 0);

            query_group = optStringVal("query_group");

            final boolean rmi = parsedCL.hasOption("rmi");
            final boolean normi = parsedCL.hasOption("no-rmi") || parsedCL.hasOption("normi");

            if (rmi && normi) {
                System.err.println("Can't have both --rmi and --no-rmi");
                System.exit(1);
                rmi_option = null;
            }
            else if (rmi)
                rmi_option = SQLConnector.RMI_YES;
            else if (normi)
                rmi_option = SQLConnector.RMI_NO;
            else if (is_admin)
                rmi_option = SQLConnector.RMI_NO;
            else
                rmi_option = SQLConnector.RMI_DEFAULT;

        }
    }

    public static void main (String[] commandLine)
            throws SQLException,ClassNotFoundException,IOException  {
        ParsedArgs args = new ParsedArgs(commandLine);
        Properties options = new Properties();
        if (args.query_group != null)
            options.put("query_group", args.query_group);

        if ("snowflake".equalsIgnoreCase(args.dbConfig.getDriver())) {
            String role = DBUserConfig.getSnowflakeRole(args.is_admin, args.user);
            if (role != null)
                options.put("role", role);
        }

        SQLConnection conn = SQLConnector.connect(args.dbConfig, args.user, null, options,
                args.dry_run, args.rmi_option, args.rmi_server, args.rs_type);

        ResultSet res = null;
        long t0 = System.currentTimeMillis();
        String q;
        if ("sqlserver".equals(conn.getDriver()))
            q = "select top " + (args.limit + 1) + " * from " + args.tableName + ((args.filter==null)?"":(" where " + args.filter));
        else
            q = "select * from " + args.tableName + ((args.filter==null)?"":(" where " + args.filter)) + " limit " + (args.limit + 1);

        try {
            res = conn.select(q);
        }
        catch (SQLException err) {
            log.info("select() caught an exception");
            System.err.println(ExceptionUtils.getRootCauseMessage(err));
            System.exit(1);
        }

        long t1 = System.currentTimeMillis();
        log.info(String.format("Took  %.3f secs", (t1 - t0)/1000.0));

        TransposedListing.run(res,args.nullString, args.keyColName, args.limit, args.getTermWidth(), args.minWidth, args.colWidth, args.outputFile);


        /*
        ResultSetMetaData rsmd = res.getMetaData();
        int nCols = rsmd.getColumnCount();
        List<CFormatter> formatters = new ArrayList<>();
        List<String> labels = new ArrayList<>();
        int keyColIdx = -1;
        for (int iCol = 1; iCol <= nCols; iCol ++) {
            CFormatter cf = CFormatter.create(rsmd, iCol, args.nullString);
            String label = rsmd.getColumnLabel(iCol).toLowerCase();
            int idx = label.indexOf('.');
            if (idx < label.length() - 2)
                label = label.substring(idx + 1,label.length());
            formatters.add(cf);
            labels.add(label);
            if (label.equalsIgnoreCase(args.keyColName))
                keyColIdx = iCol - 1;
        }

        List<List<String>> rows = new ArrayList<>();
        int nRows = 0;
        while(res.next()) {
            List<String> row = new ArrayList<>();
            nRows ++;
            for (CFormatter cf : formatters)
                row.add(cf.format(res));
            rows.add(row);
        }

        log.info(String.format("Retrieved %d columns and %d rows%s, took %.3f secs",
                nCols, nRows,
                (nRows > args.limit && args.filter != null)? " (over the limit)":"",
                (t1 - t0)/1000.0));
        if (nRows == 0) {
            conn.close();
            return;
        }

        FormattedOutput out;
        if (args.outputFile == null)
            out = FormattedOutput.newTabularOutput (System.out, args.getTermWidth(), args.minWidth, null);
        else
            out = FormattedOutput.newCSVOutput(new PrintStream(args.outputFile, StandardCharsets.UTF_8.name()),false);

        if (nRows > args.limit)
            nRows = args.limit;

        out.addColumn("column", 0, true);
        for (int ii = 0; ii < nRows; ii ++)
            out.addColumn((keyColIdx < 0)? ("<" + (ii+1) + ">"): rows.get(ii).get(keyColIdx) , args.colWidth, false);

        for (int ii = 0; ii < nCols; ii ++) {
            List<String> row = new ArrayList<>();

            row.add(labels.get(ii));
            for (int jj = 0; jj < nRows; jj ++)
                row.add(rows.get(jj).get(ii));
            out.writerow(row);
        }

        out.close ();
        */

        conn.close ();
    }
}
