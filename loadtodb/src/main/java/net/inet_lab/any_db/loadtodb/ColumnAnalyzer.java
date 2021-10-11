package net.inet_lab.any_db.loadtodb;

import java.util.Arrays;
import java.util.Date;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class ColumnAnalyzer {
    private final Consumer[] consumers = {new IntegerConsumer(),
                                          new DateConsumer(),
                                          new FloatConsumer(),
                                          new StringConsumer()};


    ColumnAnalyzer(int iCol, String nullString) {
        for (int ii = 0; ii < consumers.length; ii ++) {
            consumers[ii].iCol = iCol;
            consumers[ii].weight = 1 + ii;
            consumers[ii].nullString = nullString;
        }
    }
    
    static private abstract class Consumer implements Comparable<Consumer> {
        public int iCol;
        
        public Integer weight;
        public String nullString;
        
        protected int maxLength;
        protected boolean active;
        Consumer() {
            maxLength = 0;
            active = true;
        }
        public void feed(int iRaw, String val) {
            if (val.equals(nullString))
                return;
            int len = 0;
            try {
                len = val.getBytes("UTF-8").length;
            }
            catch (UnsupportedEncodingException err) {
                System.err.println(err);
                System.exit(1);
            }
                
            if (len > maxLength)
                maxLength = len;
            if (active)
                active = _feed(iRaw,val);
        }
        abstract protected boolean _feed(int iRaw, String val);
        public boolean isActive () {
            return active;
        }
        public int compareTo(Consumer a) {
            if (active && !a.active)
                return -1;
            if (!active && a.active)
                return 1;
            return weight.compareTo(a.weight);
        }
        public String columnDef() {
            if (!active)
                throw new RuntimeException("Column is not active");
            return _columnDef();
        }
        abstract protected String _columnDef();
    }

    static private class IntegerConsumer extends Consumer {
        private boolean ok_32;
        public IntegerConsumer () {
            ok_32 = true;
        }
        protected boolean _feed(int iRaw, String val) {
            try {
                Long.parseLong(val);
            }
            catch (NumberFormatException err) {
                return false;
            }

            if (ok_32) {
                try {
                    Integer.parseInt(val);
                }
                catch (NumberFormatException err) {
                    ok_32 = false;
                }
            }
            return true;
        }
        protected String _columnDef() {
            return ok_32? "integer" : "bigint";
        }
    }
                
    static private class DateConsumer extends Consumer {
        final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
        protected boolean _feed(int iRaw, String val) {
            try {
                Date d = DATE_FORMAT.parse(val);
                return DATE_FORMAT.format(d).equals(val);
            }
            catch (ParseException err) {
                return false;
            }
        }
        protected String _columnDef() {
            return "date";
        }
    }
                
    static private class FloatConsumer extends Consumer {
        protected boolean _feed(int iRaw, String val) {
            try {
                Double.parseDouble(val);
            }
            catch (NumberFormatException err) {
                return false;
            }
            return true;
        }
        protected String _columnDef() {
            return "float";
        }
    }
                
    static private class StringConsumer extends Consumer {
        protected boolean _feed(int iRaw, String val) {
            return true;
        }
        protected String _columnDef() {
            return "varchar(" + maxLength + ")";
        }
    }
        
    public void feed(int iRaw, String val) {
        for (int ii = 0; ii < consumers.length; ii ++)
            if (consumers[ii].isActive())
                consumers[ii].feed(iRaw,val);
    }

    public String columnDef() {
        Arrays.sort(consumers);
        return consumers[0].columnDef();
    }
}
