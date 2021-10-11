package net.inet_lab.any_db.utils;

import java.io.IOException;
import java.rmi.Remote;
import java.sql.SQLException;
import java.util.Properties;

public interface SQLRemote extends Remote {
    JConnection connect(String jdbcSchema,
                        String host, Integer port, String database,
                        String user, String pass,
                        boolean ssl, String sslfactory,
                        Properties options, String rs_type, boolean reconnect) throws IOException, SQLException;
}
