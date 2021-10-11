package net.inet_lab.any_db.loadtodb;

import java.io.FileReader;
import java.io.Reader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.File;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.regex.Pattern;
import java.util.List;
import java.util.ArrayList;
import java.sql.SQLException;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.collections.IteratorUtils;
import org.apache.log4j.Logger;
import org.apache.commons.io.input.BOMInputStream;

import com.opencsv.CSVReader;

import net.inet_lab.any_db.utils.DBConfig;
import net.inet_lab.any_db.utils.ParsedArgsBaseDBAccess;
import net.inet_lab.any_db.utils.SQLConnector;
import net.inet_lab.any_db.utils.SQLConnection;
import net.inet_lab.any_db.utils.AWSClient;

public class LoadToDB {
    private static final Logger log = Logger.getLogger(LoadToDB.class.getName());

    private static final Pattern VALID_ID = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");
    private static ParsedArgs args;

    static class ParsedArgs extends ParsedArgsBaseDBAccess {
        private final String loginUser = System.getenv().get("USER");

        final DBConfig dbConfig;
        final String csvFileName;
        final String spec;
        final Boolean hasHeader;
        final boolean dropFirst;
        final boolean append;
        final boolean autospec;
        final String user;
        final String targetTable;
        final String bucket = "ars-dev";
        final String nullString;
        final int delimiter;
        final String s3loc  = "files/" + loginUser + "/loadtodb/";

        final CSVFormat csvFormat;


        ParsedArgs(String[] commandLine) {
            StringBuilder selector = new StringBuilder();
            for (String x : DBConfig.values()) {
                if (selector.length() > 0)
                    selector.append('|');
                selector.append(x);
            }
            final String USAGE="loadtodb.sh [options] [" + selector + "] <CSV file>";
            final int WIDTH = 120;
            final String default_format = "RFC4180";
            HelpFormatter helper = new HelpFormatter();
            // https://commons.apache.org/proper/commons-cli/apidocs/index.html
            Options options = (new Options())
                    .addOption(Option.builder()
                            .longOpt("help")
                            .desc("Print this help")
                            .build())
                    .addOption(Option.builder()
                            .longOpt("header")
                            .hasArg().argName("YES|NO")
                            .desc("YES if file has header, NO otherwise (autodetected if not specified)")
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
                            .desc("user name (default = " + loginUser + ")")
                            .build())
                    .addOption(Option.builder("t")
                            .hasArg().argName("TABLE")
                            .longOpt("target")
                            .desc("target table")
                            .build())
                    .addOption(Option.builder("s")
                            .hasArg().argName("SEPARATOR")
                            .longOpt("separator")
                            .desc("Indicate separator for the source file as ASCII code, tab=9, default=44 (comma)")
                            .build())
                    .addOption(Option.builder()
                            .hasArg().argName("NULL VALUE")
                            .longOpt("null")
                            .desc("NULL value (default = NULL)")
                            .build())
                    .addOption(Option.builder()
                            .longOpt("drop")
                            .desc("drop target table first")
                            .build())
                    .addOption(Option.builder()
                            .longOpt("append")
                            .desc("append to the target table (not trying to re-create)")
                            .build())
                    .addOption(Option.builder()
                            .hasArg().argName("SPEC")
                            .longOpt("spec")
                            .desc("columnName1 columnType1, columnName2 columnType2,... ")
                            .build())
                    .addOption(Option.builder()
                            .longOpt("autospec")
                            .desc("Generate table tafinition automatically and use it right away")
                            .build())
                    .addOption(Option.builder("f")
                            .hasArg().argName("CSV FORMAT")
                            .longOpt("format")
                            .desc("One of CSVFormat.Predefined formats or 'opencsv'; default = " + default_format)
                            .build())
                    .addOption(Option.builder()
                            .hasArg().argName("CONFIG")
                            .longOpt("default-config")
                            .desc("Default config if one not specified as argument")
                            .build());

            parsedCL = parseCL(options, commandLine);

            if (parsedCL.hasOption("help")) {
                helper.printHelp(WIDTH, USAGE, "options:", options, "", false);
                System.exit(0);
            }

            append = parsedCL.hasOption("append");
            spec = parsedCL.getOptionValue("spec");
            autospec = parsedCL.hasOption("autospec");

            if (spec != null && autospec) {
                System.err.println("Can't have both --spec and --autospec, chose one");
            }

            final List<String> loArgs = new ArrayList<>();
            dbConfig = getConfig(loArgs, null);

            if (!verifyConfig(dbConfig)) {
                System.exit(1);
            }

            user = optStringVal("user", (dbConfig.default_user == null)?loginUser:dbConfig.default_user);

            if (loArgs.size() == 0) {
                helper.printHelp(WIDTH, USAGE, "options:", options, "Error: Missing argument", false);
                System.exit(1);
            }
            else if (loArgs.size() > 1) {
                helper.printHelp(WIDTH, USAGE, "options:", options, "Error: Too many arguments", false);
                System.exit(1);
            }
            csvFileName = loArgs.get(0);

            hasHeader = optBooleanVal("header");

            targetTable = parsedCL.getOptionValue("target");
            nullString = parsedCL.getOptionValue("null", "NULL");

            dropFirst = parsedCL.hasOption("drop");
            delimiter = optIntegerVal("separator", (int)',');

            CSVFormat _csvFormat = null;
            String format = parsedCL.getOptionValue("format",  default_format);
            if (!"opencsv".equalsIgnoreCase(format)) {
                try {
                    _csvFormat = CSVFormat.valueOf(format);
                }
                catch (IllegalArgumentException e) {
                    StringBuilder b = new StringBuilder();
                    b.append("OpenCSV");
                    for (CSVFormat.Predefined f : CSVFormat.Predefined.values())
                        b.append(", " + f);
                    System.err.println("Invalid value of format = " + format + ", use one of " + b.toString());
                    System.exit(1);
                }
            }
            csvFormat = _csvFormat;

            if (append && dropFirst) {
                System.err.println("ERROR: Canot use both --append and --drop");
                System.exit(1);
                return;
            }
        }
    }

    public static void main (String[] commandLine) throws SQLException,ClassNotFoundException {
        args = new ParsedArgs(commandLine);

        try {
            proceccCSV();
        }
        catch (IOException err) {
            System.err.println(err);
            System.exit(1);
        }
    }

    private static String[] getNextRow(CSVReader reader, Iterator<CSVRecord> records, int[] lineno) throws IOException {
        String [] row = null;
        if (reader == null) {
            if (records.hasNext()) {
                CSVRecord r = records.next();
                row = (String[]) IteratorUtils.toArray(r.iterator(), String.class);
            }
        }
        else
            row = reader.readNext();

        if (row != null)
            lineno[0] ++;
        return row;
    }

    private static void proceccCSV() throws SQLException,ClassNotFoundException,IOException {
        Reader in;
        try {
            //in = new FileReader(args.csvFileName);
            // excluding EF BB BF header
            in = new InputStreamReader(new BOMInputStream(new FileInputStream(args.csvFileName)));
        }
        catch (FileNotFoundException err) {
            System.err.println(err);
            System.exit(1);
            return;
        }

        CSVReader reader = null;
        Iterator<CSVRecord> records = null;

        if (args.csvFormat == null)
        // last arg is quotechar, thus effectively disabling quotes unless CSV
            reader = new CSVReader(in, (char)args.delimiter, (args.delimiter==(int)',')?'"' : (char)0);
        else
            records = args.csvFormat.parse(in).iterator();


        final String driver = args.dbConfig.getDriver();

        int[] lineno = {0};
        String [] row = getNextRow(reader, records, lineno);
        String [] header = null;

        Boolean hasHeader = args.hasHeader;
        if (hasHeader == null) {
            if (row == null) {
                System.err.println("CSV file " + args.csvFileName + " is empty");
                System.exit(1);
            }

            int ii = 0;
            while (ii < row.length && VALID_ID.matcher(row[ii]).find())
                ii ++;
            hasHeader = (ii == row.length);
            System.out.println("Headers autodetected to be " + (hasHeader?"YES":"NO") + (hasHeader?"":" (example of invalid column name: '" + row[ii] + "')"));
        }

        String spec = args.spec;

        if (!args.append && spec == null) {
            if (hasHeader) {
                header = row;
                row = getNextRow(reader, records, lineno);
            }

            if (row == null) {
                System.err.println("CSV file " + args.csvFileName + " has nothing but header");
                System.exit(1);
            }

            ColumnAnalyzer[] colA = null;
            int iraw = 0;
            do {
                iraw ++;
                if (colA == null) {
                    colA = new ColumnAnalyzer[row.length];
                    for (int ii = 0; ii < row.length; ii ++)
                        colA[ii] = new ColumnAnalyzer(ii,args.nullString);
                }
                if (row.length != colA.length) {
                    System.err.println("ERROR[" + iraw + "]: " + row.length + " columns, expecting " + colA.length);
                    int icol = 0;
                    for (String c : row) {
                        icol ++;
                        System.err.println("" + icol + ". " + c);
                    }
                    System.exit(1);
                }
                if (header != null && row.length != header.length) {
                    System.err.println("ERROR : header has " + header.length + " columns, row # " + iraw + " has " + row.length);
                    System.exit(1);
                }

                for (int ii = 0; ii < row.length; ii ++)
                    colA[ii].feed(iraw,row[ii]);

                row = getNextRow(reader, records, lineno);
            }
            while (row != null);

            System.out.println("Read " + lineno[0] + " lines");

            StringBuilder defs = new StringBuilder();

            for (int ii = 0; ii < colA.length; ii ++) {
                defs.append((header == null) ? ("column_" + ii) : header[ii]).append(" ").append(colA[ii].columnDef());
                if (ii < colA.length - 1)
                    defs.append(",");
            }

            if (args.autospec) {
                spec = defs.toString();
                System.out.println("Autogenerated spec: " + spec);
            }
            else
                System.out.println("CVS file analyzed, you can upload it with\n" + "--spec \"" + defs + "\"");
        }

        if (args.append || spec != null) {
            if (reader != null)
                reader.close ();

            if (args.targetTable == null) {
                System.err.println("Must provide target table (use -t,--target)");
                System.exit(1);
            }

            SQLConnection conn = SQLConnector.connect(args.dbConfig, args.user, null, null, false, SQLConnector.RMI_DEFAULT, null, null);

            AWSClient awsClient = new AWSClient();
            File csvFile = new File(args.csvFileName);
            String target = args.s3loc + csvFile.getName();
            String s3target = "s3://" + args.bucket + "/" + target;
            System.out.println("Copying " + args.csvFileName + " to " + s3target +
                    (hasHeader?" (without header row)":""));
            if (hasHeader) {
                long len = csvFile.length();
                FileInputStream fh = new FileInputStream(csvFile);
                do {
                    len -= 1;
                }
                while (fh.read() != '\n');
                awsClient.put(fh, len, args.bucket, target);
            }
            else {
                awsClient.put(args.csvFileName, args.bucket, target);
            }

            try {
                if (args.dropFirst) {
                    conn.execute("drop table if exists " + args.targetTable);
                }
                if (!args.append) {
                    conn.execute("create table " + args.targetTable + "(" + spec + ")");
                }
                int res = -1;
                if (driver.equalsIgnoreCase("redshift")) {
                    res = conn.execute("copy " + args.targetTable + "\n" +
                            "from '" + s3target + "'\n" +
                            "with credentials '" + awsClient.getCredentials(driver) + "'\n" +
                            "delimiter as '\\" + Integer.toOctalString(args.delimiter) + "'\n" +
                            ((args.delimiter == ',')?"csv quote as '\"'\n":"") +
                            "NULL as '" + args.nullString + "'");
                }
                else if (driver.equalsIgnoreCase("snowflake")) {
                    // CONTINUE | SKIP_FILE | SKIP_FILE_num | SKIP_FILE_num% | ABORT_STATEMENT
                    String on_error = "ABORT_STATEMENT";
                    boolean no_backslash_escape = true; // so that input like that won't choke: John\,Abram,Lev
                    res = conn.execute("copy into " + args.targetTable + "\n" +
                            "from '" + s3target + "'\n" +
                            "credentials = (" + awsClient.getCredentials(driver) + ")\n" +
                            "file_format = (type = 'CSV' " +
                                            "null_if=('"  + args.nullString + "') " +
                                            "FIELD_DELIMITER='\\" + Integer.toOctalString(args.delimiter) + "' " +
                                            "FIELD_OPTIONALLY_ENCLOSED_BY=" + ((args.delimiter == ',')?"'\"'":"NONE") + " " +
                                            (no_backslash_escape? "ESCAPE_UNENCLOSED_FIELD=None":"") +
                                            ")\n" +
                            "TRUNCATECOLUMNS = TRUE\n" +
                            "ON_ERROR = " + on_error);
                }
                else {
                    log.error("Don't know yet how to upload data to " + driver);
                }
                log.info("conn.execute() returned " + res);
            }
            catch (SQLException err) {
                System.err.println(err);
                System.exit(1);
            }
        }
    }
}
