package net.inet_lab.any_db.loadrun;

public interface DBConnector {
    String getType(String tableName, String colName);
}
