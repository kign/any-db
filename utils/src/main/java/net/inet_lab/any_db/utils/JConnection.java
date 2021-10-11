package net.inet_lab.any_db.utils;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface JConnection extends Remote {
    ResultSet executeQuery(String sql) throws SQLException,RemoteException;
    int executeUpdate(String sql) throws SQLException,RemoteException;
    void initConn(boolean force_reset) throws SQLException,RemoteException;
    String getDatabase() throws RemoteException;
    void close () throws SQLException,RemoteException;
    void commit () throws SQLException,RemoteException;
}
