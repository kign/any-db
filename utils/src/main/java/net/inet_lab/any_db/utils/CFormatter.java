package net.inet_lab.any_db.utils;

import java.sql.ResultSetMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.sql.Array;

public abstract class CFormatter {
    protected String name;
    protected String nullString;
    protected int iCol;
    protected int itype;
    protected String dbg_itypename;

    public static CFormatter create(ResultSetMetaData rsmd, int iCol, String nullString) {
        CFormatter res;
        try {
            int itype = rsmd.getColumnType(iCol);

            if (itype == Types.VARCHAR            ||
                itype == 1    /* bpchar*/         ||
                itype == -1   /* mysql varchar */ ||
                itype == -16  /* presto VACHAR */ ||
                itype == 2000 /* presto MAP(VACHAR,VACHAR) */ ||
                itype == 1111 /* postgres JSON */ ||
                itype == 2005 /* ntext @ ms-sql */)
                res = new CFormatterString ();
            else if (itype == Types.INTEGER || itype == Types.BIGINT || itype == Types.TINYINT || itype == Types.SMALLINT || itype == Types.NUMERIC)
                res = new CFormatterFixedPoint ();
            else if (itype == Types.DOUBLE || itype == Types.FLOAT || itype==7 /* float4 */ || itype==3 /* mysql decimal */)
                res = new CFormatterFloat ();
            else if (itype == Types.DATE || itype == Types.TIMESTAMP)
                res = new CFormatterString ();
            // have no idea where -7 comes from and why it is also 'bool'
            else if (itype == Types.BOOLEAN || itype==-7)
                res = new CFormatterBoolean ();
            else if (itype == Types.ARRAY || itype==2003)
                res = new CFormatterArray ();
            else
                throw new RuntimeException("No formatter for type " + itype + ", " + rsmd.getColumnTypeName(iCol));

            res.name = rsmd.getColumnName(iCol);
            res.nullString = nullString;
            res.iCol = iCol;
            res.itype = itype;
            res.dbg_itypename = rsmd.getColumnTypeName(iCol);

            return res;
        }
        catch(SQLException err) {
            throw new RuntimeException("Could not initialize formatter for column " + iCol);
        }
    }

    public String toString() {
        return getClass().getName() + "[iCol=" + iCol + ",name=" + name + ",type=" + dbg_itypename + "]";
    }

    public String format(ResultSet rs) {
        try {
            String v = _format(rs);
            if (v == null)
                return nullString;
            else
                return v;
        }
        catch (SQLException e) {
            throw new RuntimeException("Exception in formatter, column " + iCol + ", " + name + " : " + e);
        }
    }

    protected String _format(ResultSet rs) throws SQLException {
        return rs.getString(iCol);
    }

    public boolean getLeftAlign () {
        return true;
    }

    public String getFormatPattern () {
        return null;
    }

    public Object getRawData(ResultSet rs) throws SQLException {
        return rs.getString(iCol);
    }

    private static class CFormatterString extends CFormatter {
    }

    private static class CFormatterFixedPoint extends CFormatter {
        public boolean getLeftAlign () {
            return false;
        }

        public String getFormatPattern () {
            if (itype == Types.INTEGER || itype == Types.BIGINT || itype == Types.TINYINT || itype == Types.SMALLINT)
                return "0";
            else
                return "#,##0.00";
        }

        public Object getRawData(ResultSet rs) throws SQLException {
            if (rs.getString(iCol) == null)
                return null;
            else if (itype == Types.INTEGER || itype == Types.TINYINT || itype == Types.SMALLINT)
                return rs.getInt(iCol);
            else if (itype == Types.BIGINT)
                return rs.getLong(iCol);
            else
                return rs.getDouble(iCol);
        }
    }

    private static class CFormatterFloat extends CFormatter {
        public boolean getLeftAlign () {
            return true;
        }

        public String getFormatPattern () {
            return "0.000000";
        }

        public Object getRawData(ResultSet rs) throws SQLException {
            String s = rs.getString(iCol);
            if (s == null)
                return null;
            else {
                try {
                    return rs.getDouble(iCol);
                }
                catch (SQLException e) {
                }
                try {
                    return Double.parseDouble(s);
                }
                catch (NumberFormatException e) {
                    System.err.println("Column " + iCol + ", value " + s);
                    throw e;
                }
            }
        }
    }

    private static class CFormatterBoolean extends CFormatter {
        protected String _format(ResultSet rs) throws SQLException {
            boolean r = rs.getBoolean(iCol);
            if (rs.wasNull())
                return null;
            else if (r)
                return "t";
            else
                return "f";
        }

        public Object getRawData(ResultSet rs) throws SQLException {
            return rs.getBoolean(iCol);
        }
    }

    private static class CFormatterArray extends CFormatter {
        protected String _format(ResultSet rs) throws SQLException {
            Array a = rs.getArray(iCol);
            if (a == null)
                return null;
            Object[] values = (Object[])a.getArray();
            StringBuilder res = new StringBuilder();
            res.append("[");
            for (int ii = 0; ii < values.length; ii ++) {
                if (values[ii] instanceof Integer)
                    res.append(((Integer)values[ii]).toString());
                else if (values[ii] instanceof Long)
                    res.append(((Long)values[ii]).toString());
                else
                    res.append((String)values[ii]);
                if (ii < values.length - 1)
                    res.append(", ");
            }
            res.append("]");
            return res.toString();
        }

        public Object getRawData(ResultSet rs) throws SQLException {
            return rs.getArray(iCol);
        }
    }
}
