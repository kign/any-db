package net.inet_lab.any_db.jdbcserver;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Properties;

import net.inet_lab.any_db.utils.JConnection;
import net.inet_lab.any_db.utils.JDBC_Connection;
import net.inet_lab.any_db.utils.SQLRemote;
import net.inet_lab.any_db.utils.CommonParameters;

public class SQLRemoteServer implements SQLRemote {
    private final static Map<String,JConnection> connections = new HashMap<>();
    private final static Map<String,JConnection> connectionsImpl = new HashMap<>();

    // keeping a static reference to exported object prevent it from being GC'ed at the end of main ()
    private static SQLRemote stub;
    private static Registry registry;
    private static SQLRemote remoteServer;

    @Override
    public JConnection connect(String jdbcSchema, String host, Integer port, String database,
                               String user, String pass, boolean ssl, String sslfactory, Properties options,
                               String rs_type, boolean reconnect)
            throws IOException {
        String signature = String.format("jdbcSchema=%s;host=%s;port=%s;database=%s;user=%s",
                jdbcSchema, host, port, database, user);

        JConnection jConnection = connections.get(signature);

        if (jConnection == null || reconnect) {
            JConnection jConnectionImpl = new JDBC_Connection(jdbcSchema, host, port, database,
                    user, pass, ssl, sslfactory, options, rs_type, true);
            System.out.println("Allocating new connection for " + signature);
            jConnection = (JConnection) UnicastRemoteObject.exportObject(jConnectionImpl,CommonParameters.JCONN_PORT);
            System.out.println("Making new connection available on port " + CommonParameters.JCONN_PORT);

            connections.put(signature, jConnection);
            connectionsImpl.put(signature, jConnectionImpl);
        }

        return jConnection;
    }

    public static void main(String[] args) throws RemoteException {
        remoteServer = new SQLRemoteServer();
        stub = (SQLRemote) UnicastRemoteObject.exportObject(remoteServer, 0);
        registry = LocateRegistry.createRegistry(CommonParameters.REGISTRY_PORT);
        System.out.println("In-JVM registry started on port " + CommonParameters.REGISTRY_PORT);
        registry.rebind("SQLRemote", stub);
        System.out.println("SQLRemoteServer server started and accepting RMI connections");
    }
}
