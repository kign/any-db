package net.inet_lab.any_db.sqli;

import java.util.HashMap;
import java.util.Map;
import java.util.Collection;

import net.inet_lab.any_db.utils.SQLConnection;
import org.apache.log4j.Logger;

class SQLiContext {
    private static final Logger log = Logger.getLogger(SQLi.class.getName());

    private final String schema;
    private String lastCheckedTable;
    final private Map<String,String> vars;
    private boolean paramEnabled;

    SQLiContext(SQLConnection conn) {
        switch (conn.getDriver()) {
            case "presto":
                schema = "default";
                break;
            case "sqlserver":
                schema = "dbo";
                break;
            case "mysql":
                schema = conn.getDatabase();
                break;
            default :
                schema = "public";
        }
        lastCheckedTable = null;
        vars = new HashMap<>();
        paramEnabled = false;
    }

    String getSchema() {
        return schema;
    }

    String getLastCheckedTable() {
        return lastCheckedTable;
    }

    void setLastCheckedTable(String table) {
        log.debug("lastCheckedTable set to " + table);
        lastCheckedTable = table;
    }

    void setVariable(String var, String val) {
        vars.put(var, val);
    }

    void delVariable(String var) {
        vars.remove(var);
    }

    String getVariable(String var) {
        return vars.get(var);
    }

    Collection<String> getAllVariables () {
        return vars.keySet();
    }

    boolean getParamEnabled() {
        return paramEnabled;
    }

    void setParamEnabled(boolean enable) {
        log.debug("Subsitution " + (enable? "enabled": "disabled"));
        paramEnabled = enable;
    }
}
