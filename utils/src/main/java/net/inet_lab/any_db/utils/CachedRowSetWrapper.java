package net.inet_lab.any_db.utils;

import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Calendar;

import java.io.InputStream;
import java.io.Reader;
import java.io.Serializable;
import java.math.BigDecimal;
import java.net.URL;

import java.sql.Statement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Timestamp;
import java.sql.Time;
import java.sql.Savepoint;
import java.sql.Ref;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Array;
import java.sql.SQLXML;
import java.sql.RowId;
import java.sql.NClob;
import java.sql.SQLWarning;
import java.sql.Date;
import javax.sql.RowSet;
import javax.sql.RowSetEvent;
import javax.sql.RowSetListener;
import javax.sql.RowSetMetaData;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetWarning;
import javax.sql.rowset.spi.SyncProvider;
import javax.sql.rowset.spi.SyncProviderException;

import org.apache.commons.lang3.StringUtils;
import static net.inet_lab.any_db.utils.Misc.*;


public class CachedRowSetWrapper implements CachedRowSet,Serializable {
    private String user;
    private String pass;
    private String url;
    private String sql;

    private Metadata metaData;
    private List<Column[]> rows;
    int rowIndex;
    Column[] currentRow;
    int lastColIndex;

    static class Column implements Serializable {
        String xString;
        Boolean xBoolean;
        Integer xInt;
        Long xLong;
        Double xDouble;

        boolean wasNull;
    }

    static class MetaColumn implements Serializable {
        int type;
        String name;
        String typeName;
        String label;
    }

    static class Metadata implements ResultSetMetaData,Serializable {
        private final int columnCount;
        private final MetaColumn[] metaCols;

        Metadata(ResultSetMetaData meta) throws SQLException {
            columnCount = meta.getColumnCount();
            metaCols = new MetaColumn[columnCount];
            for (int iCol = 1; iCol <= columnCount; iCol ++) {
                MetaColumn metaCol = new MetaColumn ();
                metaCol.type = meta.getColumnType(iCol);
                metaCol.name = meta.getColumnName(iCol);
                metaCol.typeName = meta.getColumnTypeName(iCol);
                metaCol.label = meta.getColumnLabel(iCol);

                metaCols[iCol - 1] = metaCol;
            }

        }

        @Override
        public int getColumnCount() throws SQLException {
            return columnCount;
        }

        @Override
        public boolean isAutoIncrement(int column) throws SQLException {
            throw new RuntimeException("isAutoIncrement(int column) not implemented");
        }

        @Override
        public boolean isCaseSensitive(int column) throws SQLException {
            throw new RuntimeException("isCaseSensitive(int column) not implemented");
        }

        @Override
        public boolean isSearchable(int column) throws SQLException {
            throw new RuntimeException("isSearchable(int column) not implemented");
        }

        @Override
        public boolean isCurrency(int column) throws SQLException {
            throw new RuntimeException("isCurrency(int column) not implemented");
        }

        @Override
        public int isNullable(int column) throws SQLException {
            throw new RuntimeException("isNullable(int column) not implemented");
        }

        @Override
        public boolean isSigned(int column) throws SQLException {
            throw new RuntimeException("isSigned(int column) not implemented");
        }

        @Override
        public int getColumnDisplaySize(int column) throws SQLException {
            throw new RuntimeException("getColumnDisplaySize(int column) not implemented");
        }

        @Override
        public String getColumnLabel(int column) throws SQLException {
            return metaCols[column - 1].label;
        }

        @Override
        public String getColumnName(int column) throws SQLException {
            return metaCols[column - 1].name;
        }

        @Override
        public String getSchemaName(int column) throws SQLException {
            throw new RuntimeException("getSchemaName(int column) not implemented");
        }

        @Override
        public int getPrecision(int column) throws SQLException {
            throw new RuntimeException("getPrecision(int column) not implemented");
        }

        @Override
        public int getScale(int column) throws SQLException {
            throw new RuntimeException("getScale(int column) not implemented");
        }

        @Override
        public String getTableName(int column) throws SQLException {
            throw new RuntimeException("getTableName(int column) not implemented");
        }

        @Override
        public String getCatalogName(int column) throws SQLException {
            throw new RuntimeException("getCatalogName(int column) not implemented");
        }

        @Override
        public int getColumnType(int column) throws SQLException {
            return metaCols[column - 1].type;
        }

        @Override
        public String getColumnTypeName(int column) throws SQLException {
            return metaCols[column -1].typeName;
        }

        @Override
        public boolean isReadOnly(int column) throws SQLException {
            throw new RuntimeException("isReadOnly(int column) not implemented");
        }

        @Override
        public boolean isWritable(int column) throws SQLException {
            throw new RuntimeException("isWritable(int column) not implemented");
        }

        @Override
        public boolean isDefinitelyWritable(int column) throws SQLException {
            throw new RuntimeException("isDefinitelyWritable(int column) not implemented");
        }

        @Override
        public String getColumnClassName(int column) throws SQLException {
            throw new RuntimeException("getColumnClassName(int column) not implemented");
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new RuntimeException("unwrap(Class<T> iface) not implemented");
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            throw new RuntimeException("isWrapperFor(Class<?> iface) not implemented");
        }
    }

    @Override
    public void execute() throws SQLException {
        executeWithStatement(null);
    }

    public void executeWithStatement(Statement stmt) throws SQLException {
        Statement xstmt = stmt;
        Connection xconn = null;
        if (stmt == null) {
            System.out.println("CachedRowSetWrapper: connecting to " + url + " as " + user + ((pass == null)?" without ":" with ") + "password");
            xconn = DriverManager.getConnection(url,user,pass);
            xstmt = xconn.createStatement();
        }

        System.out.println("CachedRowSetWrapper: executing " + StringUtils.abbreviate(straightenSql(sql), 100));
        ResultSet rs = xstmt.executeQuery(sql);
        metaData = new Metadata(rs.getMetaData());

        int columnsNumber = metaData.getColumnCount();

        rows = new ArrayList<>();
        while(rs.next()) {
            Column[] cols = new Column[columnsNumber];
            for (int iCol = 1; iCol <= columnsNumber; iCol ++) {
                Column col = new Column ();

                col.xString = rs.getString(iCol);

                try {
                    col.xBoolean = rs.getBoolean(iCol);
                }
                catch (Exception err) {
                }

                try {
                    col.xInt = rs.getInt(iCol);
                }
                catch (Exception err) {
                }

                try {
                    col.xLong = rs.getLong(iCol);
                }
                catch (Exception err) {
                }

                try {
                    col.xDouble = rs.getDouble(iCol);
                }
                catch (Exception err) {
                }

                col.wasNull = rs.wasNull();
                cols[iCol - 1] = col;
            }
            rows.add(cols);
        }

        rowIndex = -1;

        if (stmt == null) {
            xstmt.close();
            xconn.close();

        }
    }

    @Override
    public boolean next() throws SQLException {
        rowIndex += 1;
        if (rowIndex < rows.size()) {
            currentRow = rows.get(rowIndex);
            return true;
        }
        return false;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        lastColIndex = columnIndex;
        return currentRow[columnIndex - 1].xString;
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        lastColIndex = columnIndex;
        return currentRow[columnIndex - 1].xBoolean;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        lastColIndex = columnIndex;
        throw new RuntimeException("getByte(int columnIndex) not implemented");
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        lastColIndex = columnIndex;
        throw new RuntimeException("getShort(int columnIndex) not implemented");
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        lastColIndex = columnIndex;
        return currentRow[columnIndex - 1].xInt;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        lastColIndex = columnIndex;
        return currentRow[columnIndex - 1].xLong;
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        lastColIndex = columnIndex;
        throw new RuntimeException("getFloat(int columnIndex) not implemented");
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        lastColIndex = columnIndex;
        return currentRow[columnIndex - 1].xDouble;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        lastColIndex = columnIndex;
        throw new RuntimeException("getBigDecimal(int columnIndex, int scale) not implemented");
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        lastColIndex = columnIndex;
        throw new RuntimeException("getBytes(int columnIndex) not implemented");
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        lastColIndex = columnIndex;
        throw new RuntimeException("getDate(int columnIndex) not implemented");
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        lastColIndex = columnIndex;
        throw new RuntimeException("getTime(int columnIndex) not implemented");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        lastColIndex = columnIndex;
        throw new RuntimeException("getTimestamp(int columnIndex) not implemented");
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        lastColIndex = columnIndex;
        throw new RuntimeException("getAsciiStream(int columnIndex) not implemented");
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        lastColIndex = columnIndex;
        throw new RuntimeException("getUnicodeStream(int columnIndex) not implemented");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        lastColIndex = columnIndex;
        throw new RuntimeException("getBinaryStream(int columnIndex) not implemented");
    }

    @Override
    public boolean wasNull() throws SQLException {
        return currentRow[lastColIndex - 1].wasNull;
    }

    @Override
    public void populate(ResultSet data) throws SQLException {
        throw new RuntimeException("populate(ResultSet data) not implemented");
    }

    @Override
    public void execute(Connection conn) throws SQLException {
        throw new RuntimeException("executeWithStatement(Connection conn) not implemented");
    }

    @Override
    public void acceptChanges() throws SyncProviderException {
        throw new RuntimeException("acceptChanges() not implemented");
    }

    @Override
    public void acceptChanges(Connection con) throws SyncProviderException {
        throw new RuntimeException("acceptChanges(Connection con) not implemented");
    }

    @Override
    public void restoreOriginal() throws SQLException {
        throw new RuntimeException("restoreOriginal() not implemented");
    }

    @Override
    public void release() throws SQLException {
        throw new RuntimeException("release() not implemented");
    }

    @Override
    public void undoDelete() throws SQLException {
        throw new RuntimeException("undoDelete() not implemented");
    }

    @Override
    public void undoInsert() throws SQLException {
        throw new RuntimeException("undoInsert() not implemented");
    }

    @Override
    public void undoUpdate() throws SQLException {
        throw new RuntimeException("undoUpdate() not implemented");
    }

    @Override
    public boolean columnUpdated(int idx) throws SQLException {
        throw new RuntimeException("columnUpdated(int idx) not implemented");
    }

    @Override
    public boolean columnUpdated(String columnName) throws SQLException {
        throw new RuntimeException("columnUpdated(String columnName) not implemented");
    }

    @Override
    public Collection<?> toCollection() throws SQLException {
        throw new RuntimeException("toCollection() not implemented");
    }

    @Override
    public Collection<?> toCollection(int column) throws SQLException {
        throw new RuntimeException("toCollection(int column) not implemented");
    }

    @Override
    public Collection<?> toCollection(String column) throws SQLException {
        throw new RuntimeException("toCollection(String column) not implemented");
    }

    @Override
    public SyncProvider getSyncProvider() throws SQLException {
        throw new RuntimeException("getSyncProvider() not implemented");
    }

    @Override
    public void setSyncProvider(String provider) throws SQLException {
        throw new RuntimeException("setSyncProvider(String provider) not implemented");
    }

    @Override
    public int size() {
        throw new RuntimeException("size() not implemented");
    }

    @Override
    public void setMetaData(RowSetMetaData md) throws SQLException {
        throw new RuntimeException("setMetaData(RowSetMetaData md) not implemented");
    }

    @Override
    public ResultSet getOriginal() throws SQLException {
        throw new RuntimeException("getOriginal() not implemented");
    }

    @Override
    public ResultSet getOriginalRow() throws SQLException {
        throw new RuntimeException("getOriginalRow() not implemented");
    }

    @Override
    public void setOriginalRow() throws SQLException {
        throw new RuntimeException("setOriginalRow() not implemented");
    }

    @Override
    public String getTableName() throws SQLException {
        throw new RuntimeException("getTableName() not implemented");
    }

    @Override
    public void setTableName(String tabName) throws SQLException {
        throw new RuntimeException("setTableName(String tabName) not implemented");
    }

    @Override
    public int[] getKeyColumns() throws SQLException {
        throw new RuntimeException("getKeyColumns() not implemented");
    }

    @Override
    public void setKeyColumns(int[] keys) throws SQLException {
        throw new RuntimeException("setKeyColumns(int[] keys) not implemented");
    }

    @Override
    public RowSet createShared() throws SQLException {
        throw new RuntimeException("createShared() not implemented");
    }

    @Override
    public CachedRowSet createCopy() throws SQLException {
        throw new RuntimeException("createCopy() not implemented");
    }

    @Override
    public CachedRowSet createCopySchema() throws SQLException {
        throw new RuntimeException("createCopySchema() not implemented");
    }

    @Override
    public CachedRowSet createCopyNoConstraints() throws SQLException {
        throw new RuntimeException("createCopyNoConstraints() not implemented");
    }

    @Override
    public RowSetWarning getRowSetWarnings() throws SQLException {
        throw new RuntimeException("getRowSetWarnings() not implemented");
    }

    @Override
    public boolean getShowDeleted() throws SQLException {
        throw new RuntimeException("getShowDeleted() not implemented");
    }

    @Override
    public void setShowDeleted(boolean b) throws SQLException {
        throw new RuntimeException("setShowDeleted(boolean b) not implemented");
    }

    @Override
    public void commit() throws SQLException {
        throw new RuntimeException("commit() not implemented");
    }

    @Override
    public void rollback() throws SQLException {
        throw new RuntimeException("rollback() not implemented");
    }

    @Override
    public void rollback(Savepoint s) throws SQLException {
        throw new RuntimeException("rollback(Savepoint s) not implemented");
    }

    @Override
    public void rowSetPopulated(RowSetEvent event, int numRows) throws SQLException {
        throw new RuntimeException("rowSetPopulated(RowSetEvent event, int numRows) not implemented");
    }

    @Override
    public void populate(ResultSet rs, int startRow) throws SQLException {
        throw new RuntimeException("populate(ResultSet rs, int startRow) not implemented");
    }

    @Override
    public void setPageSize(int size) throws SQLException {
        throw new RuntimeException("setPageSize(int size) not implemented");
    }

    @Override
    public int getPageSize() {
        throw new RuntimeException("getPageSize() not implemented");
    }

    @Override
    public boolean nextPage() throws SQLException {
        throw new RuntimeException("nextPage() not implemented");
    }

    @Override
    public boolean previousPage() throws SQLException {
        throw new RuntimeException("previousPage() not implemented");
    }

    @Override
    public String getUrl() throws SQLException {
        throw new RuntimeException("getUrl() not implemented");
    }

    @Override
    public void setUrl(String url) throws SQLException {
        this.url = url;
    }

    @Override
    public String getDataSourceName() {
        throw new RuntimeException("getDataSourceName() not implemented");
    }

    @Override
    public void setDataSourceName(String name) throws SQLException {
        throw new RuntimeException("setDataSourceName(String name) not implemented");
    }

    @Override
    public String getUsername() {
        throw new RuntimeException("getUsername() not implemented");
    }

    @Override
    public void setUsername(String name) throws SQLException {
        user = name;

    }

    @Override
    public String getPassword() {
        throw new RuntimeException("getPassword() not implemented");
    }

    @Override
    public void setPassword(String password) throws SQLException {
        pass = password;
    }

    @Override
    public int getTransactionIsolation() {
        throw new RuntimeException("getTransactionIsolation() not implemented");
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        throw new RuntimeException("setTransactionIsolation(int level) not implemented");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new RuntimeException("getTypeMap() not implemented");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new RuntimeException("setTypeMap(Map<String, Class<?>> map) not implemented");
    }

    @Override
    public String getCommand() {
        throw new RuntimeException("getCommand() not implemented");
    }

    @Override
    public void setCommand(String cmd) throws SQLException {
        sql = cmd;
    }

    @Override
    public boolean isReadOnly() {
        throw new RuntimeException("isReadOnly() not implemented");
    }

    @Override
    public void setReadOnly(boolean value) throws SQLException {
        throw new RuntimeException("setReadOnly(boolean value) not implemented");
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new RuntimeException("setMaxFieldSize(int max) not implemented");
    }

    @Override
    public int getMaxRows() throws SQLException {
        throw new RuntimeException("getMaxRows() not implemented");
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        throw new RuntimeException("setMaxRows(int max) not implemented");
    }

    @Override
    public boolean getEscapeProcessing() throws SQLException {
        throw new RuntimeException("getEscapeProcessing() not implemented");
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new RuntimeException("setEscapeProcessing(boolean enable) not implemented");
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        throw new RuntimeException("getQueryTimeout() not implemented");
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        throw new RuntimeException("setQueryTimeout(int seconds) not implemented");
    }

    @Override
    public void setType(int type) throws SQLException {
        throw new RuntimeException("setType(int type) not implemented");
    }

    @Override
    public void setConcurrency(int concurrency) throws SQLException {
        throw new RuntimeException("setConcurrency(int concurrency) not implemented");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        throw new RuntimeException("setNull(int parameterIndex, int sqlType) not implemented");
    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        throw new RuntimeException("setNull(String parameterName, int sqlType) not implemented");
    }

    @Override
    public void setNull(int paramIndex, int sqlType, String typeName) throws SQLException {
        throw new RuntimeException("setNull(int paramIndex, int sqlType, String typeName) not implemented");
    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        throw new RuntimeException("setNull(String parameterName, int sqlType, String typeName) not implemented");
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        throw new RuntimeException("setBoolean(int parameterIndex, boolean x) not implemented");
    }

    @Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        throw new RuntimeException("setBoolean(String parameterName, boolean x) not implemented");
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        throw new RuntimeException("setByte(int parameterIndex, byte x) not implemented");
    }

    @Override
    public void setByte(String parameterName, byte x) throws SQLException {
        throw new RuntimeException("setByte(String parameterName, byte x) not implemented");
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        throw new RuntimeException("setShort(int parameterIndex, short x) not implemented");
    }

    @Override
    public void setShort(String parameterName, short x) throws SQLException {
        throw new RuntimeException("setShort(String parameterName, short x) not implemented");
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        throw new RuntimeException("setInt(int parameterIndex, int x) not implemented");
    }

    @Override
    public void setInt(String parameterName, int x) throws SQLException {
        throw new RuntimeException("setInt(String parameterName, int x) not implemented");
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        throw new RuntimeException("setLong(int parameterIndex, long x) not implemented");
    }

    @Override
    public void setLong(String parameterName, long x) throws SQLException {
        throw new RuntimeException("setLong(String parameterName, long x) not implemented");
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        throw new RuntimeException("setFloat(int parameterIndex, float x) not implemented");
    }

    @Override
    public void setFloat(String parameterName, float x) throws SQLException {
        throw new RuntimeException("setFloat(String parameterName, float x) not implemented");
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        throw new RuntimeException("setDouble(int parameterIndex, double x) not implemented");
    }

    @Override
    public void setDouble(String parameterName, double x) throws SQLException {
        throw new RuntimeException("setDouble(String parameterName, double x) not implemented");
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        throw new RuntimeException("setBigDecimal(int parameterIndex, BigDecimal x) not implemented");
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        throw new RuntimeException("setBigDecimal(String parameterName, BigDecimal x) not implemented");
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        throw new RuntimeException("setString(int parameterIndex, String x) not implemented");
    }

    @Override
    public void setString(String parameterName, String x) throws SQLException {
        throw new RuntimeException("setString(String parameterName, String x) not implemented");
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw new RuntimeException("setBytes(int parameterIndex, byte[] x) not implemented");
    }

    @Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        throw new RuntimeException("setBytes(String parameterName, byte[] x) not implemented");
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        throw new RuntimeException("setDate(int parameterIndex, Date x) not implemented");
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        throw new RuntimeException("setTime(int parameterIndex, Time x) not implemented");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        throw new RuntimeException("setTimestamp(int parameterIndex, Timestamp x) not implemented");
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        throw new RuntimeException("setTimestamp(String parameterName, Timestamp x) not implemented");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new RuntimeException("setAsciiStream(int parameterIndex, InputStream x, int length) not implemented");
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        throw new RuntimeException("setAsciiStream(String parameterName, InputStream x, int length) not implemented");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new RuntimeException("setBinaryStream(int parameterIndex, InputStream x, int length) not implemented");
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        throw new RuntimeException("setBinaryStream(String parameterName, InputStream x, int length) not implemented");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new RuntimeException("setCharacterStream(int parameterIndex, Reader reader, int length) not implemented");
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        throw new RuntimeException("setCharacterStream(String parameterName, Reader reader, int length) not implemented");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new RuntimeException("setAsciiStream(int parameterIndex, InputStream x) not implemented");
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        throw new RuntimeException("setAsciiStream(String parameterName, InputStream x) not implemented");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new RuntimeException("setBinaryStream(int parameterIndex, InputStream x) not implemented");
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        throw new RuntimeException("setBinaryStream(String parameterName, InputStream x) not implemented");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new RuntimeException("setCharacterStream(int parameterIndex, Reader reader) not implemented");
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        throw new RuntimeException("setCharacterStream(String parameterName, Reader reader) not implemented");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new RuntimeException("setNCharacterStream(int parameterIndex, Reader value) not implemented");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        throw new RuntimeException("setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) not implemented");
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        throw new RuntimeException("setObject(String parameterName, Object x, int targetSqlType, int scale) not implemented");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        throw new RuntimeException("setObject(int parameterIndex, Object x, int targetSqlType) not implemented");
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        throw new RuntimeException("setObject(String parameterName, Object x, int targetSqlType) not implemented");
    }

    @Override
    public void setObject(String parameterName, Object x) throws SQLException {
        throw new RuntimeException("setObject(String parameterName, Object x) not implemented");
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        throw new RuntimeException("setObject(int parameterIndex, Object x) not implemented");
    }

    @Override
    public void setRef(int i, Ref x) throws SQLException {
        throw new RuntimeException("setRef(int i, Ref x) not implemented");
    }

    @Override
    public void setBlob(int i, Blob x) throws SQLException {
        throw new RuntimeException("setBlob(int i, Blob x) not implemented");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new RuntimeException("setBlob(int parameterIndex, InputStream inputStream, long length) not implemented");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new RuntimeException("setBlob(int parameterIndex, InputStream inputStream) not implemented");
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        throw new RuntimeException("setBlob(String parameterName, InputStream inputStream, long length) not implemented");
    }

    @Override
    public void setBlob(String parameterName, Blob x) throws SQLException {
        throw new RuntimeException("setBlob(String parameterName, Blob x) not implemented");
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        throw new RuntimeException("setBlob(String parameterName, InputStream inputStream) not implemented");
    }

    @Override
    public void setClob(int i, Clob x) throws SQLException {
        throw new RuntimeException("setClob(int i, Clob x) not implemented");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new RuntimeException("setClob(int parameterIndex, Reader reader, long length) not implemented");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new RuntimeException("setClob(int parameterIndex, Reader reader) not implemented");
    }

    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        throw new RuntimeException("setClob(String parameterName, Reader reader, long length) not implemented");
    }

    @Override
    public void setClob(String parameterName, Clob x) throws SQLException {
        throw new RuntimeException("setClob(String parameterName, Clob x) not implemented");
    }

    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {
        throw new RuntimeException("setClob(String parameterName, Reader reader) not implemented");
    }

    @Override
    public void setArray(int i, Array x) throws SQLException {
        throw new RuntimeException("setArray(int i, Array x) not implemented");
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new RuntimeException("setDate(int parameterIndex, Date x, Calendar cal) not implemented");
    }

    @Override
    public void setDate(String parameterName, Date x) throws SQLException {
        throw new RuntimeException("setDate(String parameterName, Date x) not implemented");
    }

    @Override
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        throw new RuntimeException("setDate(String parameterName, Date x, Calendar cal) not implemented");
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new RuntimeException("setTime(int parameterIndex, Time x, Calendar cal) not implemented");
    }

    @Override
    public void setTime(String parameterName, Time x) throws SQLException {
        throw new RuntimeException("setTime(String parameterName, Time x) not implemented");
    }

    @Override
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        throw new RuntimeException("setTime(String parameterName, Time x, Calendar cal) not implemented");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new RuntimeException("setTimestamp(int parameterIndex, Timestamp x, Calendar cal) not implemented");
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        throw new RuntimeException("setTimestamp(String parameterName, Timestamp x, Calendar cal) not implemented");
    }

    @Override
    public void clearParameters() throws SQLException {
        throw new RuntimeException("clearParameters() not implemented");
    }

    @Override
    public void addRowSetListener(RowSetListener listener) {
        throw new RuntimeException("addRowSetListener(RowSetListener listener) not implemented");
    }

    @Override
    public void removeRowSetListener(RowSetListener listener) {
        throw new RuntimeException("removeRowSetListener(RowSetListener listener) not implemented");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new RuntimeException("setSQLXML(int parameterIndex, SQLXML xmlObject) not implemented");
    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        throw new RuntimeException("setSQLXML(String parameterName, SQLXML xmlObject) not implemented");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new RuntimeException("setRowId(int parameterIndex, RowId x) not implemented");
    }

    @Override
    public void setRowId(String parameterName, RowId x) throws SQLException {
        throw new RuntimeException("setRowId(String parameterName, RowId x) not implemented");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new RuntimeException("setNString(int parameterIndex, String value) not implemented");
    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {
        throw new RuntimeException("setNString(String parameterName, String value) not implemented");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new RuntimeException("setNCharacterStream(int parameterIndex, Reader value, long length) not implemented");
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        throw new RuntimeException("setNCharacterStream(String parameterName, Reader value, long length) not implemented");
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        throw new RuntimeException("setNCharacterStream(String parameterName, Reader value) not implemented");
    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
        throw new RuntimeException("setNClob(String parameterName, NClob value) not implemented");
    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        throw new RuntimeException("setNClob(String parameterName, Reader reader, long length) not implemented");
    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
        throw new RuntimeException("setNClob(String parameterName, Reader reader) not implemented");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new RuntimeException("setNClob(int parameterIndex, Reader reader, long length) not implemented");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new RuntimeException("setNClob(int parameterIndex, NClob value) not implemented");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new RuntimeException("setNClob(int parameterIndex, Reader reader) not implemented");
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new RuntimeException("setURL(int parameterIndex, URL x) not implemented");
    }

    @Override
    public void close() throws SQLException {
        throw new RuntimeException("close() not implemented");
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        throw new RuntimeException("getString(String columnLabel) not implemented");
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        throw new RuntimeException("getBoolean(String columnLabel) not implemented");
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        throw new RuntimeException("getByte(String columnLabel) not implemented");
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        throw new RuntimeException("getShort(String columnLabel) not implemented");
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        throw new RuntimeException("getInt(String columnLabel) not implemented");
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        throw new RuntimeException("getLong(String columnLabel) not implemented");
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        throw new RuntimeException("getFloat(String columnLabel) not implemented");
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        throw new RuntimeException("getDouble(String columnLabel) not implemented");
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        throw new RuntimeException("getBigDecimal(String columnLabel, int scale) not implemented");
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        throw new RuntimeException("getBytes(String columnLabel) not implemented");
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        throw new RuntimeException("getDate(String columnLabel) not implemented");
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        throw new RuntimeException("getTime(String columnLabel) not implemented");
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        throw new RuntimeException("getTimestamp(String columnLabel) not implemented");
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new RuntimeException("getAsciiStream(String columnLabel) not implemented");
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new RuntimeException("getUnicodeStream(String columnLabel) not implemented");
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new RuntimeException("getBinaryStream(String columnLabel) not implemented");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new RuntimeException("getWarnings() not implemented");
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new RuntimeException("clearWarnings() not implemented");
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new RuntimeException("getCursorName() not implemented");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return metaData;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        throw new RuntimeException("getObject(int columnIndex) not implemented");
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        throw new RuntimeException("getObject(String columnLabel) not implemented");
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        throw new RuntimeException("findColumn(String columnLabel) not implemented");
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new RuntimeException("getCharacterStream(int columnIndex) not implemented");
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new RuntimeException("getCharacterStream(String columnLabel) not implemented");
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        throw new RuntimeException("getBigDecimal(int columnIndex) not implemented");
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        throw new RuntimeException("getBigDecimal(String columnLabel) not implemented");
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        throw new RuntimeException("isBeforeFirst() not implemented");
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        throw new RuntimeException("isAfterLast() not implemented");
    }

    @Override
    public boolean isFirst() throws SQLException {
        throw new RuntimeException("isFirst() not implemented");
    }

    @Override
    public boolean isLast() throws SQLException {
        throw new RuntimeException("isLast() not implemented");
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new RuntimeException("beforeFirst() not implemented");
    }

    @Override
    public void afterLast() throws SQLException {
        throw new RuntimeException("afterLast() not implemented");
    }

    @Override
    public boolean first() throws SQLException {
        throw new RuntimeException("first() not implemented");
    }

    @Override
    public boolean last() throws SQLException {
        throw new RuntimeException("last() not implemented");
    }

    @Override
    public int getRow() throws SQLException {
        throw new RuntimeException("getRow() not implemented");
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw new RuntimeException("absolute(int row) not implemented");
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new RuntimeException("relative(int rows) not implemented");
    }

    @Override
    public boolean previous() throws SQLException {
        throw new RuntimeException("previous() not implemented");
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new RuntimeException("setFetchDirection(int direction) not implemented");
    }

    @Override
    public int getFetchDirection() throws SQLException {
        throw new RuntimeException("getFetchDirection() not implemented");
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw new RuntimeException("setFetchSize(int rows) not implemented");
    }

    @Override
    public int getFetchSize() throws SQLException {
        throw new RuntimeException("getFetchSize() not implemented");
    }

    @Override
    public int getType() throws SQLException {
        throw new RuntimeException("getType() not implemented");
    }

    @Override
    public int getConcurrency() throws SQLException {
        throw new RuntimeException("getConcurrency() not implemented");
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw new RuntimeException("rowUpdated() not implemented");
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw new RuntimeException("rowInserted() not implemented");
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw new RuntimeException("rowDeleted() not implemented");
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new RuntimeException("updateNull(int columnIndex) not implemented");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new RuntimeException("updateBoolean(int columnIndex, boolean x) not implemented");
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new RuntimeException("updateByte(int columnIndex, byte x) not implemented");
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new RuntimeException("updateShort(int columnIndex, short x) not implemented");
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new RuntimeException("updateInt(int columnIndex, int x) not implemented");
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new RuntimeException("updateLong(int columnIndex, long x) not implemented");
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new RuntimeException("updateFloat(int columnIndex, float x) not implemented");
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new RuntimeException("updateDouble(int columnIndex, double x) not implemented");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new RuntimeException("updateBigDecimal(int columnIndex, BigDecimal x) not implemented");
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new RuntimeException("updateString(int columnIndex, String x) not implemented");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new RuntimeException("updateBytes(int columnIndex, byte[] x) not implemented");
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new RuntimeException("updateDate(int columnIndex, Date x) not implemented");
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new RuntimeException("updateTime(int columnIndex, Time x) not implemented");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new RuntimeException("updateTimestamp(int columnIndex, Timestamp x) not implemented");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new RuntimeException("updateAsciiStream(int columnIndex, InputStream x, int length) not implemented");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new RuntimeException("updateBinaryStream(int columnIndex, InputStream x, int length) not implemented");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new RuntimeException("updateCharacterStream(int columnIndex, Reader x, int length) not implemented");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new RuntimeException("updateObject(int columnIndex, Object x, int scaleOrLength) not implemented");
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new RuntimeException("updateObject(int columnIndex, Object x) not implemented");
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new RuntimeException("updateNull(String columnLabel) not implemented");
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new RuntimeException("updateBoolean(String columnLabel, boolean x) not implemented");
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new RuntimeException("updateByte(String columnLabel, byte x) not implemented");
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new RuntimeException("updateShort(String columnLabel, short x) not implemented");
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new RuntimeException("updateInt(String columnLabel, int x) not implemented");
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new RuntimeException("updateLong(String columnLabel, long x) not implemented");
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new RuntimeException("updateFloat(String columnLabel, float x) not implemented");
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new RuntimeException("updateDouble(String columnLabel, double x) not implemented");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new RuntimeException("updateBigDecimal(String columnLabel, BigDecimal x) not implemented");
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new RuntimeException("updateString(String columnLabel, String x) not implemented");
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new RuntimeException("updateBytes(String columnLabel, byte[] x) not implemented");
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new RuntimeException("updateDate(String columnLabel, Date x) not implemented");
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new RuntimeException("updateTime(String columnLabel, Time x) not implemented");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new RuntimeException("updateTimestamp(String columnLabel, Timestamp x) not implemented");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new RuntimeException("updateAsciiStream(String columnLabel, InputStream x, int length) not implemented");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new RuntimeException("updateBinaryStream(String columnLabel, InputStream x, int length) not implemented");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new RuntimeException("updateCharacterStream(String columnLabel, Reader reader, int length) not implemented");
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new RuntimeException("updateObject(String columnLabel, Object x, int scaleOrLength) not implemented");
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new RuntimeException("updateObject(String columnLabel, Object x) not implemented");
    }

    @Override
    public void insertRow() throws SQLException {
        throw new RuntimeException("insertRow() not implemented");
    }

    @Override
    public void updateRow() throws SQLException {
        throw new RuntimeException("updateRow() not implemented");
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new RuntimeException("deleteRow() not implemented");
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new RuntimeException("refreshRow() not implemented");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new RuntimeException("cancelRowUpdates() not implemented");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new RuntimeException("moveToInsertRow() not implemented");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new RuntimeException("moveToCurrentRow() not implemented");
    }

    @Override
    public Statement getStatement() throws SQLException {
        throw new RuntimeException("getStatement() not implemented");
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new RuntimeException("getObject(int columnIndex, Map<String, Class<?>> map) not implemented");
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new RuntimeException("getRef(int columnIndex) not implemented");
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new RuntimeException("getBlob(int columnIndex) not implemented");
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new RuntimeException("getClob(int columnIndex) not implemented");
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new RuntimeException("getArray(int columnIndex) not implemented");
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new RuntimeException("getObject(String columnLabel, Map<String, Class<?>> map) not implemented");
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new RuntimeException("getRef(String columnLabel) not implemented");
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new RuntimeException("getBlob(String columnLabel) not implemented");
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new RuntimeException("getClob(String columnLabel) not implemented");
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new RuntimeException("getArray(String columnLabel) not implemented");
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw new RuntimeException("getDate(int columnIndex, Calendar cal) not implemented");
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw new RuntimeException("getDate(String columnLabel, Calendar cal) not implemented");
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new RuntimeException("getTime(int columnIndex, Calendar cal) not implemented");
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw new RuntimeException("getTime(String columnLabel, Calendar cal) not implemented");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw new RuntimeException("getTimestamp(int columnIndex, Calendar cal) not implemented");
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw new RuntimeException("getTimestamp(String columnLabel, Calendar cal) not implemented");
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw new RuntimeException("getURL(int columnIndex) not implemented");
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw new RuntimeException("getURL(String columnLabel) not implemented");
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new RuntimeException("updateRef(int columnIndex, Ref x) not implemented");
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new RuntimeException("updateRef(String columnLabel, Ref x) not implemented");
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new RuntimeException("updateBlob(int columnIndex, Blob x) not implemented");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new RuntimeException("updateBlob(String columnLabel, Blob x) not implemented");
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new RuntimeException("updateClob(int columnIndex, Clob x) not implemented");
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new RuntimeException("updateClob(String columnLabel, Clob x) not implemented");
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new RuntimeException("updateArray(int columnIndex, Array x) not implemented");
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new RuntimeException("updateArray(String columnLabel, Array x) not implemented");
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new RuntimeException("getRowId(int columnIndex) not implemented");
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new RuntimeException("getRowId(String columnLabel) not implemented");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new RuntimeException("updateRowId(int columnIndex, RowId x) not implemented");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new RuntimeException("updateRowId(String columnLabel, RowId x) not implemented");
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new RuntimeException("getHoldability() not implemented");
    }

    @Override
    public boolean isClosed() throws SQLException {
        throw new RuntimeException("isClosed() not implemented");
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new RuntimeException("updateNString(int columnIndex, String nString) not implemented");
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new RuntimeException("updateNString(String columnLabel, String nString) not implemented");
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new RuntimeException("updateNClob(int columnIndex, NClob nClob) not implemented");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new RuntimeException("updateNClob(String columnLabel, NClob nClob) not implemented");
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new RuntimeException("getNClob(int columnIndex) not implemented");
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new RuntimeException("getNClob(String columnLabel) not implemented");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new RuntimeException("getSQLXML(int columnIndex) not implemented");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new RuntimeException("getSQLXML(String columnLabel) not implemented");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new RuntimeException("updateSQLXML(int columnIndex, SQLXML xmlObject) not implemented");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new RuntimeException("updateSQLXML(String columnLabel, SQLXML xmlObject) not implemented");
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        throw new RuntimeException("getNString(int columnIndex) not implemented");
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        throw new RuntimeException("getNString(String columnLabel) not implemented");
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new RuntimeException("getNCharacterStream(int columnIndex) not implemented");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new RuntimeException("getNCharacterStream(String columnLabel) not implemented");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new RuntimeException("updateNCharacterStream(int columnIndex, Reader x, long length) not implemented");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new RuntimeException("updateNCharacterStream(String columnLabel, Reader reader, long length) not implemented");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new RuntimeException("updateAsciiStream(int columnIndex, InputStream x, long length) not implemented");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new RuntimeException("updateBinaryStream(int columnIndex, InputStream x, long length) not implemented");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new RuntimeException("updateCharacterStream(int columnIndex, Reader x, long length) not implemented");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new RuntimeException("updateAsciiStream(String columnLabel, InputStream x, long length) not implemented");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new RuntimeException("updateBinaryStream(String columnLabel, InputStream x, long length) not implemented");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new RuntimeException("updateCharacterStream(String columnLabel, Reader reader, long length) not implemented");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new RuntimeException("updateBlob(int columnIndex, InputStream inputStream, long length) not implemented");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new RuntimeException("updateBlob(String columnLabel, InputStream inputStream, long length) not implemented");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new RuntimeException("updateClob(int columnIndex, Reader reader, long length) not implemented");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new RuntimeException("updateClob(String columnLabel, Reader reader, long length) not implemented");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new RuntimeException("updateNClob(int columnIndex, Reader reader, long length) not implemented");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new RuntimeException("updateNClob(String columnLabel, Reader reader, long length) not implemented");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new RuntimeException("updateNCharacterStream(int columnIndex, Reader x) not implemented");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new RuntimeException("updateNCharacterStream(String columnLabel, Reader reader) not implemented");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new RuntimeException("updateAsciiStream(int columnIndex, InputStream x) not implemented");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new RuntimeException("updateBinaryStream(int columnIndex, InputStream x) not implemented");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new RuntimeException("updateCharacterStream(int columnIndex, Reader x) not implemented");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new RuntimeException("updateAsciiStream(String columnLabel, InputStream x) not implemented");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new RuntimeException("updateBinaryStream(String columnLabel, InputStream x) not implemented");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new RuntimeException("updateCharacterStream(String columnLabel, Reader reader) not implemented");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new RuntimeException("updateBlob(int columnIndex, InputStream inputStream) not implemented");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new RuntimeException("updateBlob(String columnLabel, InputStream inputStream) not implemented");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new RuntimeException("updateClob(int columnIndex, Reader reader) not implemented");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new RuntimeException("updateClob(String columnLabel, Reader reader) not implemented");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new RuntimeException("updateNClob(int columnIndex, Reader reader) not implemented");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new RuntimeException("updateNClob(String columnLabel, Reader reader) not implemented");
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new RuntimeException("getObject(int columnIndex, Class<T> type) not implemented");
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        throw new RuntimeException("getObject(String columnLabel, Class<T> type) not implemented");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new RuntimeException("unwrap(Class<T> iface) not implemented");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new RuntimeException("isWrapperFor(Class<?> iface) not implemented");
    }

    @Override
    public void setMatchColumn(int columnIdx) throws SQLException {
        throw new RuntimeException("setMatchColumn(int columnIdx) not implemented");
    }

    @Override
    public void setMatchColumn(int[] columnIdxes) throws SQLException {
        throw new RuntimeException("setMatchColumn(int[] columnIdxes) not implemented");
    }

    @Override
    public void setMatchColumn(String columnName) throws SQLException {
        throw new RuntimeException("setMatchColumn(String columnName) not implemented");
    }

    @Override
    public void setMatchColumn(String[] columnNames) throws SQLException {
        throw new RuntimeException("setMatchColumn(String[] columnNames) not implemented");
    }

    @Override
    public int[] getMatchColumnIndexes() throws SQLException {
        throw new RuntimeException("getMatchColumnIndexes() not implemented");
    }

    @Override
    public String[] getMatchColumnNames() throws SQLException {
        throw new RuntimeException("getMatchColumnNames() not implemented");
    }

    @Override
    public void unsetMatchColumn(int columnIdx) throws SQLException {
        throw new RuntimeException("unsetMatchColumn(int columnIdx) not implemented");
    }

    @Override
    public void unsetMatchColumn(int[] columnIdxes) throws SQLException {
        throw new RuntimeException("unsetMatchColumn(int[] columnIdxes) not implemented");
    }

    @Override
    public void unsetMatchColumn(String columnName) throws SQLException {
        throw new RuntimeException("unsetMatchColumn(String columnName) not implemented");
    }

    @Override
    public void unsetMatchColumn(String[] columnName) throws SQLException {
        throw new RuntimeException("unsetMatchColumn(String[] columnName) not implemented");
    }
}
