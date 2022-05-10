package net.inet_lab.any_db.jrun;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.IOException;
import java.io.PrintStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;

import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import org.apache.log4j.Logger;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.lang3.exception.ExceptionUtils;

import net.inet_lab.any_db.utils.SQLConnector;
import net.inet_lab.any_db.utils.SQLConnection;
import net.inet_lab.any_db.utils.CFormatter;
import net.inet_lab.any_db.utils.DBConfig;
import net.inet_lab.any_db.utils.DBUserConfig;
import net.inet_lab.any_db.utils.ParsedArgsBaseDBAccess;
import net.inet_lab.any_db.utils.ScriptUtils;
import net.inet_lab.any_db.utils.FormattedOutput;

import static net.inet_lab.any_db.utils.Misc.*;

public class JRun {
    private static final Logger log = Logger.getLogger(JRun.class.getName());

    private static ParsedArgs args;

    static class ParsedArgs extends ParsedArgsBaseDBAccess {
        final String loginUser = System.getenv().get("USER");

        final DBConfig dbConfig;
        final String user;

        final List<String> query_a;

        final boolean dry_run;
        final boolean tuples_only;
        final boolean isUpdateQuery;
        final Map<Integer,String[]> colFormat;
        final String query_group;
        final String md_flavor;
        final String rmi_option;
        final String rmi_server = "localhost";
        final String rs_type;
        final boolean print_full_error;
        final boolean is_admin;

        final String nullString;
        final String outputFile;
        final String extension;
        final boolean unescaped;
        final boolean single;
        final String schema;

        final int minWidth = 30;

        ParsedArgs(String[] commandLine) throws IOException {
            StringBuilder selector = new StringBuilder();
            for (String x : DBConfig.values()) {
                if (selector.length() > 0)
                    selector.append('|');
                selector.append(x);
            }
            final String USAGE="jrun.sh [options] [" + selector + "] [variable=substitution] <query>";

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
                    .addOption(Option.builder("t")
                            .longOpt("no-headers")
                            .desc("Skip header row when writing to CVS file")
                            .build())
                    .addOption(Option.builder("u")
                            .longOpt("update")
                            .desc("Run as an 'update' query")
                            .build())
                    .addOption(Option.builder("O")
                            .hasArg().argName("FILE")
                            .longOpt("output")
                            .desc("Output file name")
                            .build())
                    .addOption(Option.builder("e")
                            .hasArg().argName("EXT")
                            .longOpt("extension")
                            .desc("Output file extension (will cause formatted output to STDOUT)")
                            .build())
                    .addOption(Option.builder()
                            .hasArg().argName("NULL VALUE")
                            .longOpt("null")
                            .desc("NULL value (default = NULL)")
                            .build())
                    .addOption(Option.builder("f")
                            .hasArg().argName("FORMAT")
                            .longOpt("format")
                            .desc("Column format (e.g. '1=bold,cyan 2=red')")
                            .build())
                    .addOption(Option.builder("m")
                            .hasArg().argName("MARKDOWN FLAVOUR")
                            .longOpt("markdown")
                            .desc("E.g. github, jira")
                            .build())
                    .addOption(Option.builder("q")
                            .hasArg().argName("QUERY_GROUP")
                            .longOpt("query_group")
                            .desc("Query group")
                            .build())
                    .addOption(Option.builder()
                            .longOpt("unescaped")
                            .desc("No escaping in CSV file generation; might not open in Excel correctly, but better for line by line comparison")
                            .build())
                    .addOption(Option.builder()
                            .longOpt("single")
                            .desc("Treat whole file as one large query, don't attempt to split")
                            .build())
                    .addOption(Option.builder()
                            .longOpt("nosub")
                            .desc("No paramater substitution")
                            .build())
                    .addOption(Option.builder()
                            .longOpt("rmi")
                            .desc("Force RMI use (always execute in server process)")
                            .build())
                    .addOption(Option.builder()
                            .longOpt("no-rmi")
                            .desc("Inhibit RMI use (never execute in server process)")
                            .build())
                    .addOption(Option.builder()
                            .longOpt("full-error")
                            .desc("Print full error message and Java stack on SQL exception")
                            .build())
                    .addOption(Option.builder()
                            .hasArg().argName("RS_TYPE")
                            .longOpt("rs-type")
                            .desc("Testing only; can be 'default', 'wrapper' or 'cached'")
                            .build())
                    .addOption(Option.builder("n")
                            .longOpt("dry-run")
                            .desc("Dry run mode (clusters should still be available)")
                            .build())
                    .addOption(Option.builder("o")
                            .hasArg().argName("SCHEMA")
                            .longOpt("schema")
                            .desc("use this value to replace /*{*/<schema>/*}*/")
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
            final Map<String,String> vars = new HashMap<>();
            dbConfig = getConfig(loArgs, vars);

            if (dbConfig == null) {
                System.err.println("No database definition");
                System.exit(1);
            }

            if (!verifyConfig(dbConfig)) {
                System.exit(1);
            }

            if (loArgs.size() == 0) {
                helper.printHelp(getTermWidth(), USAGE, "options:", options, "Error: Missing argument", false);
                System.exit(1);
            }
            else if (loArgs.size() > 1) {
                helper.printHelp(getTermWidth(), USAGE, "options:", options, "Error: Too many arguments", false);
                System.exit(1);
            }

            String pre_query = loArgs.get(0);
            boolean inline_query = true;
            if (pre_query.length() < 512) {
                Path p = Paths.get(pre_query);
                if(Files.isRegularFile(p)) {
                    log.debug("Reading query from file " + pre_query);
                    byte[] encoded = Files.readAllBytes(p);
                    pre_query = new String(encoded, StandardCharsets.UTF_8);
                    inline_query = false;
                }
            }
            if (inline_query) {
                // We allow to quote column names with single quote on input for simplicity; replace them now
                pre_query = pre_query.replaceAll("as +'([^']+)'","as \"$1\"");
            }

            // copy-paste from editors could yield ASCII character 160 (unbreakable space)
            pre_query = pre_query.replaceAll("\u00a0", " ");

            is_admin = parsedCL.hasOption("admin");
            if (is_admin && !"snowflake".equalsIgnoreCase(dbConfig.getDriver()))
                log.warn("--admin/-M only effective for snowflake, not " + dbConfig.getDriver());

            user = optStringVal("user", (dbConfig.default_user == null)?loginUser:dbConfig.default_user);

            // remove GRANT OWNERSHIP unless in ADMIN mode
            if (!is_admin) {
                final String pre_query_bak = pre_query;
                Pattern alter = Pattern.compile("^\\s*grant\\s+ownership\\s.+$", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
                pre_query = alter.matcher(pre_query_bak).replaceAll("");
                if (!pre_query.equals(pre_query_bak))
                    log.info("Removed 'grant ownership' statement (not admin mode)");
            }

            // remove ALTER SESSION
            if (true) {
                final String pre_query_bak = pre_query;
                Pattern alter = Pattern.compile("^\\s*alter\\s+session\\s.+$", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
                pre_query = alter.matcher(pre_query_bak).replaceAll("");
                if (!pre_query.equals(pre_query_bak))
                    log.info("Removed 'alter session' statement (universal)");
            }

            // remove alter table....
            single = parsedCL.hasOption("single");
            if (single && !"master".equalsIgnoreCase(user)) {
                final String pre_query_bak = pre_query;
                Pattern alter = Pattern.compile("^\\s*alter\\s+table\\s.+$", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
                pre_query = alter.matcher(pre_query_bak).replaceAll("");
                if (!pre_query.equals(pre_query_bak))
                    log.info("Removed 'alter table' statement");
            }

            if (!parsedCL.hasOption("nosub")) {
                try {
                    pre_query = substituteParameters(pre_query, new ParametersAccess() {
                        @Override
                        public String get(String par) {
                            return vars.get(par);
                        }
                    });
                } catch (ParametersError err) {
                    System.err.println("ERROR: " + err);
                    System.exit(1);
                }
            }

            schema = optStringVal("schema");
            if (schema != null) {
                Pattern p = Pattern.compile("/\\Q*{*/\\E[a-zA-Z0-9_]+\\Q/*}*/\\E");
                Matcher m = p.matcher(pre_query);
                pre_query = m.replaceAll(schema);
            }

            query_a = new ArrayList<>();
            if (single)
                query_a.add(pre_query);
            else
                ScriptUtils.splitSqlScript(pre_query, query_a);


            dry_run = parsedCL.hasOption("dry-run");
            tuples_only = parsedCL.hasOption("no-headers");

            nullString = parsedCL.getOptionValue("null", "NULL");

            extension = parsedCL.getOptionValue("extension");

            if (extension == null)
                outputFile = parsedCL.getOptionValue("output");
            else {
                String ofile = parsedCL.getOptionValue("output");
                if (ofile != null) {
                    log.error("Options -O and -o are incompatible.");
                    System.exit(1);
                }
                outputFile = "FAKE." + extension;
            }

            if (outputFile != null &&
                    !outputFile.endsWith(".xlsx") &&
                    !outputFile.endsWith(".txt") &&
                    !outputFile.endsWith(".md") &&
                    !outputFile.endsWith(".tsv") &&
                    !outputFile.endsWith(".csv")) {
                log.error("Unrecognized format of output file, use one of: *.csv *.xlsx *.txt *.tsv *.md");
                System.exit(1);
            }

            isUpdateQuery = parsedCL.hasOption("update");
            query_group = optStringVal("query_group");
            md_flavor = optStringVal("markdown");
            unescaped = parsedCL.hasOption("unescaped");

            colFormat = new HashMap<>();
            String f = parsedCL.getOptionValue("format");
            if (f != null) {
                Pattern p_colfmt = Pattern.compile("^(\\d+)=(([a-zA-Z0-9_]+,)*[a-zA-Z0-9_]+).*");
                int ii = 0;
                while (ii<f.length()) {
                    Matcher m = p_colfmt.matcher(f.substring(ii));
                    if (m.matches()) {
                        ii += m.end(2);
                        colFormat.put(Integer.valueOf(m.group(1)), m.group(2).split(","));
                    }
                    else {
                        System.err.println("Cannot parse format string " + f + " from position " + (1+ii));
                        System.exit(1);
                    }
                    while(ii<f.length() && f.charAt(ii)==' ')
                        ii ++;
                }
            }

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


            String x_rs_type = optStringVal("rs-type");
            rs_type = (x_rs_type == null)? null: x_rs_type.toUpperCase();

            print_full_error = parsedCL.hasOption("full-error");

            if (false) {
                for (Map.Entry<Integer,String[]> e : colFormat.entrySet()) {
                    System.out.print(e.getKey() + ": ");
                    boolean frst=true;
                    for (String fmtopt : e.getValue()) {
                        if(frst)
                            frst=false;
                        else
                            System.out.print(", ");
                        System.out.print(fmtopt);
                    }
                    System.out.println();
                }
            }
        }
    }

    public static void main (String[] commandLine)
            throws SQLException,ClassNotFoundException,IOException {
        long t0;

        args = new ParsedArgs(commandLine);
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

        conn.setMarkdownFlavor(args.md_flavor);

        int n_upd = args.isUpdateQuery ? args.query_a.size() : args.query_a.size() - 1;
        for (int ii = 0; ii < n_upd; ii++) {
            int nRes = 0;

            t0 = System.currentTimeMillis();
            try {
                nRes = conn.execute(args.query_a.get(ii));

            } catch (SQLException err) {
                System.err.println(ExceptionUtils.getRootCauseMessage(err));
                System.exit(1);
            }

            log.info(String.format("execute() returned %d; took %.3f secs", nRes, (System.currentTimeMillis() - t0) / 1000.0));
        }

        if (args.isUpdateQuery) {
            t0 = System.currentTimeMillis();
            conn.close();
            log.info(String.format("close() returned, took %.3f secs", (System.currentTimeMillis() - t0) / 1000.0));
            return;
        }

        ResultSet res = null;
        t0 = System.currentTimeMillis();
        try {
            res = conn.select(args.query_a.get(args.query_a.size()-1));
        }
        catch (SQLException err) {
            log.info("select() caught an exception");
            if (args.print_full_error)
                throw err;
            else
                System.err.println(ExceptionUtils.getRootCauseMessage(err));
            System.exit(1);
        }
        long t1 = System.currentTimeMillis();
        if (args.dry_run)
            return;
        ResultSetMetaData rsmd = res.getMetaData();
        int nCols = rsmd.getColumnCount();
        List<CFormatter> formatters = new ArrayList<>();
        FormattedOutput out;
        boolean isExcel = false;
        if (args.outputFile == null)
            out = FormattedOutput.newTabularOutput (System.out, args.getTermWidth(), args.minWidth, args.md_flavor);
        else {
            final PrintStream ps = (args.extension == null)?
                        new PrintStream(args.outputFile,StandardCharsets.UTF_8.name())
                        :System.out;
            if (args.outputFile.endsWith(".csv") && args.unescaped)
                out = FormattedOutput.newTabbedOutput(ps, args.tuples_only, ',');
            else if (args.outputFile.endsWith(".csv"))
                out = FormattedOutput.newCSVOutput(ps, args.tuples_only);
            else if (args.outputFile.endsWith(".xlsx")) {
                out = FormattedOutput.newExcelOutput(new FileOutputStream(args.outputFile));
                isExcel = true;
            }
            else if (args.outputFile.endsWith(".tsv") || args.outputFile.endsWith(".txt")) {
                out = FormattedOutput.newTabbedOutput(ps, args.tuples_only, '\t');
            }
            else if (args.outputFile.endsWith(".md")) {
                out = FormattedOutput.newMarkupOutput(ps,args.tuples_only);
            }
            else {
                log.error("Unrecognized format of output file, use one of: *.csv *.xlsx");
                System.exit(1);
                return;
            }
        }

        for (int iCol = 1; iCol <= nCols; iCol ++) {
            CFormatter cf = CFormatter.create(rsmd, iCol, args.nullString);
            String label = rsmd.getColumnLabel(iCol).toLowerCase();
            int idx = label.indexOf('.');
            if (idx < label.length() - 2)
                label = label.substring(idx + 1,label.length());
            out.addColumn(label, 0, cf.getLeftAlign(), cf.getFormatPattern(), args.colFormat.get(iCol));
            formatters.add(cf);
        }
        int nRows = 0;
        final long ts0 = System.currentTimeMillis();
        while(res.next()) {
            nRows ++;
            if (isExcel) {
                List<Object> row = new ArrayList<>();
                for (CFormatter cf : formatters) {
                    Object raw = cf.getRawData(res);
                    if (raw==null)
                        row.add(args.nullString);
                    else
                        row.add(raw);
                }
                out.writerow_excel(row);
            }
            else {
                List<String> row = new ArrayList<>();
                for (CFormatter cf : formatters)
                    row.add(cf.format(res));
                out.writerow(row);
            }
        }
        final long ts1 = System.currentTimeMillis();
        log.info(String.format("%s %d rows, took %.3f + %.3f secs",
                                (args.outputFile == null)? "Retrieved" : "Saved",
                                nRows,
                                (t1 - t0)/1000.0,
                                (ts1 - ts0)/1000.0));
        if (nRows == 0)
            out.drop ();
        else
            out.close ();
        t0 = System.currentTimeMillis();
        conn.close ();
        log.info(String.format("close() returned, took %.3f secs", (System.currentTimeMillis() - t0)/1000.0));
    }
}
