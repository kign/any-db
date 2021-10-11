package net.inet_lab.any_db.sqli;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;

import java.rmi.RemoteException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.common.collect.ImmutableMap;
import org.apache.log4j.Logger;

import net.inet_lab.any_db.utils.SQLConnection;
import net.inet_lab.any_db.utils.SQLConnector;

import static net.inet_lab.any_db.utils.Misc.*;

class SQLDict {
    private static final Logger log = Logger.getLogger(SQLDict.class.getName());

    private static final Map<String, String> colMapperSnowflake =
            ImmutableMap.of("number(38,0)", "integer",
                            "varchar(16777216)", "string");
    private static final Map<String, String> colMapperPresto =
            ImmutableMap.of("varchar", "string");

    private final SQLConnection conn;

    private Collection<Schema> cached_schemas = null;
    private final Map<String, Collection<Table>> cached_tables = new HashMap<>();
    private final Map<String, Collection<Column>> cached_cols = new HashMap<>();

    static class Error extends Exception {
        Error (String msg) {
            super(msg);
        }
    }

    static class Schema {
        String name;
        String owner;
    }

    enum Type {
        TABLE,
        VIEW
    }

    static class Table {
        String name;
        String owner;
        Type type;
        Long rows;
        Long size;
    }

    static class Column {
        String name;
        String type;
        boolean nullable;
        String more;
    }

    SQLDict(SQLConnection conn) {
        this.conn = conn;
    }

    Collection<Schema> getSchemas (boolean use_cache) throws SQLException, IOException, Error {
        if (use_cache && cached_schemas != null)
            return cached_schemas;
        List<Schema> schemas = new ArrayList<>();
        String driver = conn.getDriver();

        if ("snowflake".equals(driver)) {
            ResultSet res = conn.select("show schemas", SQLConnector.SQLPRN_LOG);
            Map<String, Integer> idx = getIndeces(res, "name", "owner");

            while (res.next()) {
                Schema schema = new Schema();
                schema.name = getLowerCaseString(res, idx.get("name"));
                schema.owner = getLowerCaseString(res, idx.get("owner"));

                schemas.add(schema);
            }
        }
        else if ("presto".equals(driver)) {
            ResultSet res = conn.select("show schemas", SQLConnector.SQLPRN_LOG);
            Map<String, Integer> idx = getIndeces(res, "schema");

            while (res.next()) {
                Schema schema = new Schema();
                schema.name = getLowerCaseString(res, idx.get("schema"));
                schema.owner = null;

                schemas.add(schema);
            }
        }
        else if ("mysql".equals(driver)) {
            ResultSet res = conn.select("show schemas", SQLConnector.SQLPRN_LOG);
            Map<String, Integer> idx = getIndeces(res, "database");

            while (res.next()) {
                Schema schema = new Schema();
                schema.name = getLowerCaseString(res, idx.get("database"));
                schema.owner = null;

                schemas.add(schema);
            }
        }
        else if ("sqlserver".equals(driver)) {
            ResultSet res = conn.select("select name from sys.schemas", SQLConnector.SQLPRN_LOG);
            Map<String, Integer> idx = getIndeces(res, "name");

            while (res.next()) {
                Schema schema = new Schema();
                schema.name = getLowerCaseString(res, idx.get("name"));
                schema.owner = null;

                schemas.add(schema);
            }
        }
        else if ("redshift"   .equals(driver)   ||
                "postgresql" .equals(driver) ) {
            ResultSet res = conn.select("select nspname, usename from pg_catalog.pg_namespace join pg_catalog.pg_user on usesysid=nspowner order by 1;",
                    SQLConnector.SQLPRN_LOG);
            Map<String, Integer> idx = getIndeces(res, "nspname", "usename");

            while (res.next()) {
                Schema schema = new Schema();
                schema.name = res.getString(idx.get("nspname"));
                schema.owner = res.getString(idx.get("usename"));

                schemas.add(schema);
            }
        }
        else {
            throw new Error("getSchemas not implemented for " + driver);
        }

        cached_schemas = schemas;
        return schemas;
    }

    Collection<Table> getTables (String schema, boolean use_cache) throws SQLException, IOException, Error {
        if (use_cache && cached_tables.containsKey(schema))
            return cached_tables.get(schema);
        List<Table> tables = new ArrayList<>();
        String driver = conn.getDriver();

        if ("snowflake".equals(driver)) {
            ResultSet res = conn.select("show tables in schema " + schema, SQLConnector.SQLPRN_LOG);
            Map<String, Integer> idx = getIndeces(res, "name", "owner", "kind", "rows", "bytes");

            while (res.next()) {
                Table table = new Table();
                table.name = getLowerCaseString(res, idx.get("name"));
                table.owner = getLowerCaseString(res, idx.get("owner"));
                table.type = Type.valueOf(res.getString(idx.get("kind")));
                table.rows = res.getLong(idx.get("rows"));
                table.size = res.getLong(idx.get("bytes"));

                tables.add(table);
            }

            res = conn.select("show views in schema " + schema, SQLConnector.SQLPRN_LOG);
            idx = getIndeces(res, "name", "owner");

            while (res.next()) {
                Table table = new Table();
                table.name = getLowerCaseString(res, idx.get("name"));
                table.owner = getLowerCaseString(res, idx.get("owner"));
                table.type = Type.VIEW;

                tables.add(table);
            }
        }
        else if ("sqlserver".equals(driver) ||
                 "mysql".equals(driver) ||
                 "presto".equals(driver)) {
            ResultSet res = conn.select(
                    String.format(
                            "select table_name, table_type from information_schema.tables where table_schema='%s'", schema),
                    SQLConnector.SQLPRN_LOG);
            Map<String, Integer> idx = getIndeces(res, "table_name", "table_type");

            while (res.next()) {
                Table table = new Table();
                table.name = getLowerCaseString(res, idx.get("table_name"));
                table.owner = null;
                String type = getLowerCaseString(res, idx.get("table_type"));
                if ("base table".equals(type))
                    table.type = Type.TABLE;
                else if ("view".equals(type))
                    table.type = Type.VIEW;
                else
                    throw new Error("Unknown table type " + type);
                table.rows = null;
                table.size = null;

                tables.add(table);
            }
        }
        // this should no longer be necessary, above clause covers this use case
        else if ("presto".equals(driver ) ||
                 "mysql".equals(driver) ) {
            ResultSet res = conn.select("show tables from " + schema, SQLConnector.SQLPRN_LOG);
            Map<String, Integer> idx = getIndeces(res, "table*");

            while (res.next()) {
                Table table = new Table();
                table.name = getLowerCaseString(res, idx.get("table*"));
                table.owner = null;
                table.type = Type.TABLE;
                table.rows = null;
                table.size = null;

                tables.add(table);
            }
        }
        else if ("redshift"   .equals(driver)   ||
                "postgresql" .equals(driver) ) {
            ResultSet res = conn.select("select tablename,tableowner from pg_catalog.pg_tables where schemaname='"
                    + schema + "' order by 1;", SQLConnector.SQLPRN_LOG);
            Map<String, Integer> idx = getIndeces(res, "tablename", "tableowner");

            while (res.next()) {
                Table table = new Table();
                table.name = res.getString(idx.get("tablename"));
                table.owner = res.getString(idx.get("tableowner"));
                table.type = Type.TABLE;
                table.rows = null;
                table.size = null;

                tables.add(table);
            }

            res = conn.select("select viewname, viewowner from pg_catalog.pg_views where schemaname='"
                    + schema + "' order by 1;", SQLConnector.SQLPRN_LOG);
            idx = getIndeces(res, "viewname", "viewowner");

            while (res.next()) {
                Table table = new Table();
                table.name = res.getString(idx.get("viewname"));
                table.owner = res.getString(idx.get("viewowner"));
                table.type = Type.VIEW;
                table.rows = null;
                table.size = null;

                tables.add(table);
            }
        }
        else {
            throw new Error("getTables not implemented for " + driver);
        }

        cached_tables.put(schema, tables);
        return tables;
    }

    Collection<Column> getColumns (String tname, String defaultSchema, boolean use_cache) throws SQLException, IOException, Error {
        if (use_cache && cached_cols.containsKey(tname))
            return cached_cols.get(tname);
        List<Column> cols = new ArrayList<>();
        String driver = conn.getDriver();

        int ii = tname.lastIndexOf('.');
        String tschema, tbl;
        if (ii == -1) {
            tschema = defaultSchema;
            tbl = tname;
        }
        else {
            tschema = tname.substring(0, ii);
            tbl = tname.substring(ii + 1);
        }

        if ("snowflake".equals(driver)) {
            ResultSet res = conn.select("desc table " + tname, SQLConnector.SQLPRN_LOG);
            Map<String, Integer> idx = getIndeces(res, "name", "type", "kind", "null?");

            while (res.next()) {
                Column col = new Column();
                col.name = getLowerCaseString(res, idx.get("name"));
                String type = getLowerCaseString(res, idx.get("type"));
                col.type = accessMap(colMapperSnowflake, type, type);

                String kind = res.getString(idx.get("kind"));

                if (!"COLUMN".equalsIgnoreCase(kind))
                    throw new RuntimeException("New kind of column: table = " + tname +
                            ", column = " + col.name + ", type = " + type + ", kind = " + kind);

                String nullable = getLowerCaseString(res, idx.get("null?"));
                if ("y".equals(nullable))
                    col.nullable = true;
                else if ("n".equals(nullable))
                    col.nullable = false;
                else
                    throw new RuntimeException("New kind of nullable: table = " + tname +
                            ", column = " + col.name + ", type = " + type + ", nullable = " + nullable);

                col.more = "";

                cols.add(col);
            }
        }
        else if ("presto".equals(driver)) {
            ResultSet res = conn.select("show columns from " + tname, SQLConnector.SQLPRN_LOG);
            Map<String, Integer> idx = getIndeces(res, "column", "type", "extra");

            while (res.next()) {
                Column col = new Column();
                col.name = getLowerCaseString(res, idx.get("column"));
                String type = getLowerCaseString(res, idx.get("type"));
                col.type = accessMap(colMapperPresto, type, type);
                col.nullable = true;
                col.more =  getLowerCaseString(res, idx.get("extra"));

                cols.add(col);
            }
        }
        else if ("mysql".equals(driver)) {
            ResultSet res = conn.select("desc " + tname, SQLConnector.SQLPRN_LOG);
            Map<String, Integer> idx = getIndeces(res, "field", "type", "null", "extra", "key");

            while (res.next()) {
                Column col = new Column();
                col.name = getLowerCaseString(res, idx.get("field"));
                col.type = getLowerCaseString(res, idx.get("type"));
                col.nullable = getLowerCaseString(res, idx.get("null")).equals("yes");
                col.more =  getLowerCaseString(res, idx.get("extra"));

                String key = getLowerCaseString(res, idx.get("key"));
                if (key != null) {
                    if ("pri".equals(key))
                        key = "primary key";
                    else if ("uni".equals(key))
                        key = "unique";

                    if (col.more == null || col.more.equals(""))
                        col.more = "";
                    else
                        col.more += "; ";
                    col.more += key;
                }

                cols.add(col);
            }
        }
        else if ("sqlserver".equals(driver)) {
            ResultSet res = conn.select(
                    String.format(
                            "select * from information_schema.columns where table_schema='%s' and table_name='%s' order by ordinal_position",
                            tschema, tbl),
                    SQLConnector.SQLPRN_LOG);
            Map<String, Integer> idx = getIndeces(res, "column_name", "data_type", "is_nullable", "character_maximum_length", "numeric_precision", "numeric_scale");

            while (res.next()) {
                Column col = new Column();
                col.name = getLowerCaseString(res, idx.get("column_name"));
                String type = getLowerCaseString(res, idx.get("data_type"));
                int len = res.getInt(idx.get("character_maximum_length"));
                boolean len_is_null = res.getString(idx.get("character_maximum_length")) == null;
                int pre = res.getInt(idx.get("numeric_precision"));
                boolean pre_is_null = res.getString(idx.get("numeric_precision")) == null;
                int sca = res.getInt(idx.get("numeric_scale"));
                boolean sca_is_null = res.getString(idx.get("numeric_scale")) == null;

                switch(type) {
                    case "int":
                    case "smallint":
                    case "tinyint":
                    case "bigint":
                        col.type = type + "(" + pre + ")";
                        break;
                    case "nvarchar":
                    case "varchar":
                        col.type = type + "(" + len + ")";
                        break;
                    case "numeric":
                    case "decimal":
                        col.type = type + "(" + pre + "," + sca + ")";
                        break;
                    case "float" :
                        if (sca == 0)
                            col.type = type + "(" + pre + ")";
                        else
                            col.type = type + "(" + pre + "," + sca + ")";
                        break;
                    default:
                        col.type = type;

                        if (!len_is_null)
                            throw new Error("Column " + col.name + " has type " + type + " and non-NULL character_maximum_length = " + len);
                        if (!pre_is_null || !sca_is_null)
                            throw new Error("Column " + col.name + " has type " + type + " and non-NULL numeric_precision/scale = " + pre + "/" + sca);
                }

                col.nullable = !getLowerCaseString(res, idx.get("is_nullable")).equals("no");
                col.more =  "";

                cols.add(col);
            }
        }
        else if ("redshift"   .equals(driver)   ||
                 "postgresql" .equals(driver) ) {

            String q = String.format("SELECT c.oid\n" +
                    "FROM pg_catalog.pg_class c\n" +
                    "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n" +
                    "WHERE c.relname OPERATOR(pg_catalog.~) '^(%s)$'\n" +
                    "  AND n.nspname OPERATOR(pg_catalog.~) '^(%s)$';", tbl, tschema);
            ResultSet res = conn.select(q, SQLConnector.SQLPRN_LOG);
            Set<String> oid_a = new HashSet<>();
            while(res.next())
                oid_a.add(res.getString(1));
            if (oid_a.isEmpty())
                throw new Error("Cannot find table '" + tschema + "." + tbl + "' in pg_catalog.pg_class");
            if (oid_a.size() > 1)
                throw new Error("Found " + oid_a.size() + " competing oid's for '" + tschema + "." + tbl + "' in pg_catalog.pg_class");

            String oid = oid_a.iterator().next();

            q = String.format("SELECT a.attname,\n" +
                    "  pg_catalog.format_type(a.atttypid, a.atttypmod),\n" +
                    "  (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)\n" +
                    "   FROM pg_catalog.pg_attrdef d\n" +
                    "   WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),\n" +
                    "  a.attnotnull, a.attnum,\n" +
                    "  NULL AS attcollation,\n" +
                    "  ''::pg_catalog.char AS attidentity,\n" +
                    "  NULL AS indexdef,\n" +
                    "  NULL AS attfdwoptions\n" +
                    "FROM pg_catalog.pg_attribute a\n" +
                    "WHERE a.attrelid = '%s' AND a.attnum > 0 AND NOT a.attisdropped\n" +
                    "ORDER BY a.attnum;", oid);
            res = conn.select(q, SQLConnector.SQLPRN_LOG);
            Map<String, Integer> idx = getIndeces(res, "attname", "format_type", "attnotnull");

            while (res.next()) {
                Column col = new Column();
                col.name = res.getString(idx.get("attname"));
                col.type = res.getString(idx.get("format_type")).replace("character varying", "varchar");
                col.nullable = !res.getBoolean(idx.get("attnotnull"));
                col.more = null;

                cols.add(col);
            }
        }
        else {
            throw new Error("getColumns not implemented for " + driver);
        }
        cached_cols.put(tname, cols);
        return cols;
    }

    public String globalColNameReplaceSnowflake(String sql) {
        for (Map.Entry<String,String> e : colMapperSnowflake.entrySet()) {
            sql = sql.replace(e.getKey(),e.getValue());
        }
        return sql;
    }

    static Map<String,Integer> getIndeces(ResultSet res, String... cols) throws SQLException {
        ResultSetMetaData rsmd = res.getMetaData();
        Map<String,Integer> indeces = new HashMap<>();

        int nCols = rsmd.getColumnCount();
        for (int iCol = 1; iCol <= nCols; iCol ++) {
            String label = rsmd.getColumnLabel(iCol);
            for (String col : cols) {
                if (col.endsWith("*") &&
                        label.toLowerCase().startsWith(col.toLowerCase().substring(0,col.length()-1)) ||
                    col.equalsIgnoreCase(label))
                    indeces.put(col, iCol);
            }
        }

        for (String col : cols) {
            if (!indeces.containsKey(col))
                throw new RuntimeException("Can't find column " + col);
        }

        return indeces;
    }

    private static String getLowerCaseString(ResultSet res, int iCol) throws SQLException {
        String s = res.getString(iCol);
        if (s == null)
            return null;
        return s.toLowerCase();
    }


}
