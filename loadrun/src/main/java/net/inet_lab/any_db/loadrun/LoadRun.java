package net.inet_lab.any_db.loadrun;

import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

import org.apache.log4j.Logger;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.HelpFormatter;

import com.amazonaws.services.s3.model.S3ObjectSummary;

import net.inet_lab.any_db.utils.SQLConnector;
import net.inet_lab.any_db.utils.SQLConnection;
import net.inet_lab.any_db.utils.DBConfig;
import net.inet_lab.any_db.utils.AWSClient;
import net.inet_lab.any_db.utils.AWSManifest;

import net.inet_lab.any_db.utils.ParsedArgsBaseDBAccess;
import static net.inet_lab.any_db.utils.Misc.*;

// https://community.cloudera.com/t5/Batch-SQL-Apache-Hive/Loading-to-S3-Fails-CDH-5-3-0/td-p/24088/page/3

/* Note: to map to Snowflake "array" column, use this expression

  case when COLUMN is null then NULL else to_json(COLUMN) end

where "to_json" defined as follows:

   CREATE TEMPORARY FUNCTION to_json AS 'brickhouse.udf.json.ToJsonUDF' USING JAR 'hdfs://leo/common/lib/3rdparty/brickhouse-0.7.1-SNAPSHOT.jar'
*/

public class LoadRun {
    private static final String JDBC_HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";

    private static final Logger log = Logger.getLogger(LoadRun.class.getName());
    private static final Random random = new Random ();

    private static ParsedArgs args;

    enum Stage {
        HIVE, DISTCP, MANIFEST, LOAD;

        public static Stage get(String val) {
            return valueOf(val.toUpperCase());
        }

        public static String getPrintedList() {
            StringBuilder sb = new StringBuilder();
            Stage[] stages = Stage.values();
            for (int ii = 0; ii < stages.length; ii ++) {
                sb.append(stages[ii].name().toLowerCase());
                if(ii < stages.length - 1)
                    sb.append(",");
            }
            return sb.toString();
        }
    }

    static class ParsedArgs extends ParsedArgsBaseDBAccess {
        final String loginUser = System.getenv().get("USER");

        final String hive_host;
        final Integer hive_port = 10000;
        final String hive_database = "default";
        final String queue;

        final DBConfig dbConfig;
        final String user;
        final boolean dropFirst;
        final boolean doAppend;

        final String query;
        final String qprefix;
        final String spec;
        final String targetTable;

        final String bucket = "individual-users";
        final String s3loc  = loginUser + "/loadrun/";
        final String hdfsloc  =  "hdfs://leo/user/" + loginUser + "/loadrun/";
        final int key;

        final Stage stage;
        final boolean dry_run;
        final boolean autospec;

        final boolean verify_spec;

        public final int minWidth = 30;

        ParsedArgs(String[] commandLine) throws IOException {
            StringBuilder selector = new StringBuilder();
            for (String x : DBConfig.values()) {
                if (selector.length() > 0)
                    selector.append('|');
                selector.append(x);
            }
            final String USAGE="loadrun.sh [options] [" + selector + "] [variable=substitution] <query or file>";

            HelpFormatter helper = new HelpFormatter();
            String default_queue = System.getenv().get("HSQL_DEFAULT_QUEUE");
            if (default_queue == null)
                default_queue = "default";

            // https://commons.apache.org/proper/commons-cli/apidocs/index.html
            Options options = (new Options())
                    .addOption(Option.builder()
                            .longOpt("help")
                            .desc("Print this help")
                            .build())
                    .addOption(Option.builder()
                            .hasArg().argName("HIVE_HOST")
                            .longOpt("hive-host")
                            .desc("hive host name")
                            .build())
                    .addOption(Option.builder("h")
                            .hasArg().argName("HOST")
                            .longOpt("host")
                            .desc("host name")
                            .build())
                    .addOption(Option.builder("q")
                            .hasArg().argName("QUEUE")
                            .longOpt("queue")
                            .desc("queue name (default = '" + default_queue + "', from $HSQL_DEFAULT_QUEUE)")
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
                    .addOption(Option.builder()
                            .longOpt("nosub")
                            .desc("No paramater substitution")
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
                    .addOption(Option.builder()
                            .longOpt("drop")
                            .desc("drop target table first")
                            .build())
                    .addOption(Option.builder("a")
                            .longOpt("append")
                            .desc("Append data to the table (you don't need spec if you use this)")
                            .build())
                    .addOption(Option.builder("s")
                            .hasArg().argName("STAGE")
                            .longOpt("stage")
                            .desc("Begin from a stage; use one of : " + Stage.getPrintedList())
                            .build())
                    .addOption(Option.builder("k")
                            .hasArg().argName("NUMBER")
                            .longOpt("key")
                            .desc("key number to detemine hdfs/s3 locations; required if using --stage, otherwise would be randomly assigned if not provided")
                            .build())
                    .addOption(Option.builder()
                            .longOpt("no-verify")
                            .desc("Do not try to verify the schema (otherwise will try to create test table, which might take time)")
                            .build())
                    .addOption(Option.builder("n")
                            .longOpt("dry-run")
                            .desc("Dry run mode (clusters should still be available)")
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

            if (!verifyConfig(dbConfig)) {
                System.exit(1);
            }

            // FIXME: use proper configuration
            hive_host = parsedCL.getOptionValue("hive-host", "my_hive_host");
            queue = parsedCL.getOptionValue("queue", default_queue);
            spec = parsedCL.getOptionValue("spec");
            autospec = parsedCL.hasOption("autospec");

            if (spec != null && autospec) {
                System.err.println("Can't have both --spec and --autospec, chose one");
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
            if (pre_query.length() < 512) {
                Path p = Paths.get(pre_query);
                if(Files.isRegularFile(p)) {
                    log.debug("Reading query from file " + pre_query);
                    byte[] encoded = Files.readAllBytes(p);
                    pre_query = new String(encoded, StandardCharsets.UTF_8);
                    Pattern p_tmr = Pattern.compile("^\\s*(set|insert) .+$", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
                    pre_query = p_tmr.matcher(pre_query).replaceAll("");
                }
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

            Pattern p_select = Pattern.compile("^\\s*select\\s", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
            Matcher m_select = p_select.matcher(pre_query);
            if (m_select.find()) {
                final int s = m_select.start();
                query = pre_query.substring(s);
                qprefix = pre_query.substring(0,s).trim();
                if (qprefix.length() > 0)
                    log.info("Query has prefix: " + qprefix);
            }
            else {
                log.error("Cannot find SELECT in supplied query, going ahead assuming it is actually there with no prefix");
                query = pre_query;
                qprefix = "";
            }

            user = optStringVal("user", loginUser);

            targetTable = parsedCL.getOptionValue("target");
            String stageName = parsedCL.getOptionValue("stage");
            if (stageName == null)
                stage = Stage.HIVE;
            else
                stage = Stage.get(stageName);

            dropFirst = parsedCL.hasOption("drop");
            doAppend = parsedCL.hasOption("append");

            dry_run = parsedCL.hasOption("dry-run");

            if (dropFirst && doAppend) {
                log.error("Cannot have both --drop and --append");
                System.exit(1);
            }

            verify_spec = (stage.compareTo(Stage.MANIFEST) < 0) && !doAppend && !parsedCL.hasOption("no-verify");

            String sKey = parsedCL.getOptionValue("key");
            if (sKey == null && stage.compareTo(Stage.HIVE) > 0) {
                key = 0;
                log.error("Must specify --key based on earlier run");
                System.exit(1);
            }
            else if (sKey == null)
                key = random.nextInt(999999);
            else
                key = Integer.parseInt(sKey);

            log.info("key = " + key);
            if (stageName == null)
                log.info("If your task is interrupted, you can resumed it from specific stage using this command:\nloadrun.sh " + restoreCL + " -k " + key + " -s [distcp|manifest|load]");
        }
    }

    static private class HiveDBConnector implements DBConnector {
        private final Map<String,Map<String,String>> desc;
        private final Statement stmt;
        HiveDBConnector(Statement stmt) {
            desc = new HashMap<>();
            this.stmt = stmt;
        }
        public String getType(String tableName, String colName) {
            if (!desc.containsKey(tableName)) {
                try {
                    Map<String,String> tdesc = new HashMap<>();

                    String sql = "desc " + tableName;
                    log.debug("Running SQL: " + sql);
                    ResultSet rs = stmt.executeQuery(sql);

                    while(rs.next()) {
                        String col_name = rs.getString("col_name");
                        String data_type = rs.getString("data_type");
                        tdesc.put(col_name,data_type);
                    }
                    desc.put(tableName, tdesc);

                    rs.close ();
                }
                catch(SQLException err) {
                    log.error("Error trying to get description for table " + tableName + " : " + err);
                    desc.put(tableName, null);
                }
            }
            if (desc.get(tableName) == null)
                return null;
            return desc.get(tableName).get(colName);
        }
    }

    public static void main (String[] commandLine) throws SQLException,ClassNotFoundException,IOException,InterruptedException  {
        args = new ParsedArgs(commandLine);

        // Connect to hive
        Class.forName(JDBC_HIVE_DRIVER);
        Properties properties = new Properties();
        if (!"default".equals(args.queue))
            properties.put("mapred.job.queue.name", args.queue);
        properties.put("user", args.loginUser);
        String jdbc_hive_url = "jdbc:hive2://" + args.hive_host + ":" + args.hive_port + "/" + args.hive_database;
        log.debug("Connecting to " + jdbc_hive_url);
        log.debug("Properties: " + properties);
        Connection con = DriverManager.getConnection(jdbc_hive_url, properties);
        Statement stmt = con.createStatement();

        final String driver = args.dbConfig.getDriver();

        if (args.spec == null && !args.doAppend) {
            String spec = AnalyzeSql.getSpec (50, new HiveDBConnector(stmt), args.query, driver);
            if (spec == null)
                System.out.println("Could not auto-generate table definition, you can fix your query or try running it anyway with --spec option");
            else if(args.autospec)
                System.out.println("Autogenerated spec: " + spec);
            else {
                System.out.println("Query analyzed, you can upload it to DB with\n" + "--spec \"" + spec + "\"\n... Or use options \"-t <target> --append\" if target table already exists");
                return;
            }
        }

        if (args.targetTable == null) {
            System.err.println("Must provide target table (use -t,--target)");
            System.exit(1);
        }

        // Connect to AWS
        AWSClient awsClient = new AWSClient();

        String dirName = args.targetTable + "_" + args.key;

        // Verify connection to DB and spec (if needed)
        SQLConnection conn = connectToDB();
        if (args.verify_spec) {
            Random r = new Random ();
            String test_table = dirName;
            conn.execute("create table " + test_table + "(" + args.spec + ")");
            conn.execute("drop table " + test_table);
        }
        conn.close ();

        String hdfstarget = args.hdfsloc + dirName;
        if (args.stage.compareTo(Stage.HIVE) <= 0) {
            stmt.execute("set mapred.job.queue.name=" + args.queue);
            stmt.execute("set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec");
            stmt.execute("set fs.s3n.awsAccessKeyId=" + awsClient.getAccessKeyId());
            stmt.execute("set fs.s3n.awsSecretAccessKey=" + awsClient.getSecretAccessKey());
            if (args.qprefix.length()>1)
                stmt.execute(args.qprefix.substring(0, args.qprefix.length()-1));

            String insertQuery = "INSERT OVERWRITE DIRECTORY '" + hdfstarget + "'\n" + args.query;
            System.out.println("set mapred.job.queue.name=" + args.queue + ";\n" +
                    "set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;\n" +
                    "set fs.s3n.awsAccessKeyId=" + awsClient.getAccessKeyId() + ";\n" +
                    "set fs.s3n.awsSecretAccessKey=" + awsClient.getSecretAccessKey() + ";\n" +
                    args.qprefix + "\n" +
                    insertQuery);
            int upd = 0;
            if (!args.dry_run)
                upd = stmt.executeUpdate(insertQuery);
            log.debug("executeUpdate returned " + upd);
        }


        if (args.stage.compareTo(Stage.DISTCP) <= 0) {
            List<String> distcp = Arrays.asList("hadoop", "distcp",
                    "-Dfs.s3n.awsAccessKeyId=" + awsClient.getAccessKeyId(),
                    "-Dfs.s3n.awsSecretAccessKey=" + awsClient.getSecretAccessKey(),
                    "-overwrite",
                    hdfstarget + "/",
                    "s3n://" + args.bucket + "/" + args.s3loc + dirName);
            log.debug(Arrays.toString(distcp.toArray()));
            int exitVal = 0;
            if (!args.dry_run) {
                Process proc = new ProcessBuilder(distcp).inheritIO().start();
                exitVal = proc.waitFor();
            }
            log.info("distcp completed with exist value " + exitVal);
        }

        final String s3manifest = args.s3loc + dirName + "/" + args.targetTable + ".manifest";

        if (!args.dry_run && args.stage.compareTo(Stage.MANIFEST) <= 0 && "redshift".equalsIgnoreCase(driver)) {
            AWSManifest manifest = new AWSManifest ();
            String s3target = "s3://" + args.bucket + "/" + args.s3loc + dirName;
            log.debug("Listing " + s3target);
            String prefix = args.s3loc + dirName + "/";
            for (S3ObjectSummary obj : awsClient.list(args.bucket, prefix)) {
                String key = obj.getKey();
                String ekey = key.substring(prefix.length());
                if (ekey.indexOf('/') < 0 && ekey.endsWith(".gz"))
                    manifest.entries.add(new AWSManifest.Entry("s3://" + args.bucket + "/" + key));
            }
            String jsonManifest = manifest.toJson();
            byte[] baJsonManifest = jsonManifest.getBytes();
            awsClient.put (new ByteArrayInputStream(baJsonManifest), baJsonManifest.length, args.bucket, s3manifest);
        }

        if (args.stage.compareTo(Stage.LOAD) <= 0) {
            conn = connectToDB();
            if (args.dropFirst)
                conn.execute("drop table if exists " + args.targetTable);
            if (!args.doAppend)
                conn.execute("create table " + args.targetTable + "(" + args.spec + ")");
            final String delimiter = "\\001";
            final String nullString = "\\\\N";
            int res = -1;
            if (driver.equalsIgnoreCase("redshift")) {
                res = conn.execute("copy " + args.targetTable + "\n" +
                        "from '" + "s3://" + args.bucket + "/" + s3manifest + "'\n" +
                        "with credentials '" + awsClient.getCredentials(driver) + "'\n" +
                        "delimiter as '" + delimiter + "'\n" +
                        "truncatecolumns\n" +
                        "NULL as '" + nullString + "'" + "\n" +
                        "GZIP\n" +
                        "MANIFEST");
            }

            /*
%%% desc file format whampipe_csv_export %%%
            property            | property_type | property_value | property_default
--------------------------------+---------------+----------------+------------------
 TYPE                           | String        | CSV            | CSV
 RECORD_DELIMITER               | String        | \n             | \n
 FIELD_DELIMITER                | String       *| 0x0001         | ,
 FILE_EXTENSION                 | String        |                |
 SKIP_HEADER                    | Integer       | 0              | 0
 DATE_FORMAT                    | String        | AUTO           | AUTO
 TIME_FORMAT                    | String        | AUTO           | AUTO
 TIMESTAMP_FORMAT               | String        | AUTO           | AUTO
 BINARY_FORMAT                  | String        | HEX            | HEX
 ESCAPE                         | String        | NONE           | NONE
 ESCAPE_UNENCLOSED_FIELD        | String       *| NONE           | \\
 TRIM_SPACE                     | Boolean      *| true           | false
 FIELD_OPTIONALLY_ENCLOSED_BY   | String        | NONE           | NONE
 NULL_IF                        | List         *| [\u0000]       | [\\N]
 COMPRESSION                    | String       *| GZIP           | AUTO
 ERROR_ON_COLUMN_COUNT_MISMATCH | Boolean       | true           | true
 VALIDATE_UTF8                  | Boolean       | true           | true
 EMPTY_FIELD_AS_NULL            | Boolean      *| false          | true
 SKIP_BYTE_ORDER_MARK           | Boolean       | true           | true
 ENCODING                       | String        | UTF8           | UTF8
             */

            else if (driver.equalsIgnoreCase("snowflake")) {
                // CONTINUE | SKIP_FILE | SKIP_FILE_num | SKIP_FILE_num% | ABORT_STATEMENT
                // See all options here: https://docs.snowflake.net/manuals/sql-reference/sql/copy-into-table.html#optional-parameters
                String on_error = "ABORT_STATEMENT";
                res = conn.execute("copy into " + args.targetTable + "\n" +
                        "from '" + "s3://" + args.bucket + "/" + args.s3loc + dirName + "/" + "'\n" +
                        "credentials = (" + awsClient.getCredentials(driver) + ")\n" +
                        "file_format = (type = 'CSV' compression = GZIP null_if=('"  + nullString + "') FIELD_DELIMITER='" + delimiter + "'" + " ESCAPE=NONE ESCAPE_UNENCLOSED_FIELD=NONE" + ")\n" +
                        "pattern = '.*.gz'\n" +
                        "TRUNCATECOLUMNS = TRUE\n" +
                        "ON_ERROR = " + on_error);
            }
            else {
                log.error("Don't know yet how to upload data to " + driver);
            }
            log.info("conn.execute() returned " + res);
            conn.close ();
        }

        log.info("hdfs dfs -rmr " + hdfstarget + "/");
        log.info("aws s3 rm --recursive " +  "s3://" + args.bucket + "/" + args.s3loc + dirName + "/");
    }

    private static SQLConnection connectToDB() throws SQLException,
            ClassNotFoundException,
            IOException {
        return SQLConnector.connect(args.dbConfig, args.user, null, null, args.dry_run, SQLConnector.RMI_DEFAULT, null, null);
    }
}
