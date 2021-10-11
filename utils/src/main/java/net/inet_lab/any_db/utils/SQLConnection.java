package net.inet_lab.any_db.utils;

import java.io.IOException;
import java.rmi.RemoteException;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface SQLConnection {
    void setMarkdownFlavor(String md_flavor);
    void consumate() throws SQLException, RemoteException;
    ResultSet select(String sql) throws SQLException, IOException;
    ResultSet select(String sql, String sqlprn) throws SQLException,IOException;
    int execute(String sql) throws SQLException, RemoteException;
    String getDriver();
    String getSignature();
    String getDatabase();
    void reset () throws SQLException, RemoteException;
    void close () throws SQLException, RemoteException;
}
