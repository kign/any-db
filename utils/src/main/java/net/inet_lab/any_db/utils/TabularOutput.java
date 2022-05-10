package net.inet_lab.any_db.utils;

import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.io.PrintStream;

import com.google.common.collect.ImmutableMap;

import org.apache.log4j.Logger;

public class TabularOutput extends FormattedOutput {
    private static final Map<String,String> ANSI_CODES = new ImmutableMap.Builder<String,String>()
        .put("reset",  "\u001B[0m")
        .put("black",  "\u001B[30m")
        .put("red",    "\u001B[31m")
        .put("green",  "\u001B[32m")
        .put("yellow", "\u001B[33m")
        .put("blue",   "\u001B[34m")
        .put("purple", "\u001B[35m")
        .put("cyan",   "\u001B[36m")
        .put("white",  "\u001B[37m")
        .build();
    private static final Logger log = Logger.getLogger(TabularOutput.class.getName());
    private final List<Column> columns;
    private final List<List<String>> rows;
    private final int termWidth;
    private final int minMaxWidth;
    private final PrintStream printStream;
    private final String md_flavor;
    private int nCols;
    private boolean closed;

    static private class Column {
        public static final int MIN_WIDTH = 2;
        public final String[] fmtopts;
        public final String   header;
        public final int      maxWidth;
        public final boolean  leftAlign;
        public       int      dataWidth;
        public       int      width;

        public Column(String header, int maxWidth, boolean leftAlign, String[] fmtopts) {
            this.header = header;
            this.maxWidth = maxWidth;
            this.leftAlign = leftAlign;
            this.fmtopts = fmtopts;

            dataWidth = header.length();
            width = MIN_WIDTH;
        }

        public String format() {
            if (fmtopts == null)
                return "";
            StringBuilder res = new StringBuilder();
            for (String fmtopt : fmtopts) {
                String ansiCode = ANSI_CODES.get(fmtopt);
                if (ansiCode == null) {
                    System.err.println("Invalid code " + fmtopt + ", please consider adding it to TabularOutput.java");
                    System.exit(1);
                }
                res.append(ansiCode);
            }
            return res.toString();
        }

        public String reset() {
            return ANSI_CODES.get("reset");
        }
    }

    public TabularOutput (PrintStream printStream, int termWidth, int minMaxWidth, String md_flavor) {
        columns = new ArrayList<>();
        rows = new ArrayList<>();
        this.termWidth = termWidth;
        this.minMaxWidth = minMaxWidth;
        this.printStream = printStream;
        this.md_flavor = md_flavor;
        closed = false;
        nCols = 0;
    }

    public void addColumn(String header, int width, boolean leftAlign, String formatPattern, String[] fmtopts) {
        log.debug("addColumn(" + header + ", " + width + ", " + leftAlign + ")");
        columns.add(new Column(header, width, leftAlign, fmtopts));
        nCols ++;
    }

    public void writerow(List<String> row) {
        int ii = 0;
        Iterator<Column> icol = columns.iterator();
        for(String v : row) {
            ii += 1;
            if (ii > nCols)
                throw new RuntimeException("Expecting " + nCols + " columns, got " + row.size());
            Column col = icol.next();
            int len = v.length ();
            if (len > col.dataWidth)
                col.dataWidth = len;
        }
        if (ii < nCols)
            throw new RuntimeException("Expecting " + nCols + " columns, got " + row.size());
        rows.add(row);
    }

    public void close () {
        if (closed)
            throw new RuntimeException("already closed");
        int nFill = 0, tWidth = 0;
        for (Column col : columns) {
            col.width = col.dataWidth;
            if (col.maxWidth > 0 ) {
                if (col.width > col.maxWidth)
                    col.width = col.maxWidth;
            }
            else {
                if (col.width > minMaxWidth) {
                    col.width = minMaxWidth;
                    nFill ++;
                }
            }
            if (col.width < Column.MIN_WIDTH)
                col.width = Column.MIN_WIDTH;
            tWidth += col.width;
        }

        if (nFill > 0 ) {
            if (termWidth <= 0 )
                throw new RuntimeException("We have " + nFill + " filled column(s) but terminal width not set");
            int rem = termWidth - tWidth - 3 * nCols + 1;
            while (rem > 0 && nFill > 0) {
                nFill = 0;
                for (Column col : columns) {
                    if (col.maxWidth == 0 ) {
                        if (col.width < col.dataWidth) {
                            col.width ++;
                            rem --;
                            if (rem == 0)
                                break;
                        }
                        if (col.width < col.dataWidth)
                            nFill += 1;
                    }
                }
            }
        }

        StringBuffer b = new StringBuffer ();
        b.append("Widths: ");
        for (Column col : columns)
            b.append(" " + col.width + "(" + col.dataWidth + ")");
        log.debug(b);

        String prefix = "github".equals(md_flavor)?"| ": "";

        StringBuilder line = new StringBuilder();
        int iCol = 0;
        for (Column col : columns) {
            line.append(" " + print(col.header,col.width,1) + " ");
            if (iCol < nCols - 1)
                line.append("|");
            iCol ++;
        }
        printStream.println(prefix + line);
        line.setLength(0);
        iCol = 0;
        for (Column col : columns) {
            line.append(mult("-",col.width + 2));
            if (iCol < nCols - 1)
                line.append(("github".equals(md_flavor))?"|":"+");
            iCol ++;
        }
        printStream.println(prefix + line);

        for (List<String> row : rows) {
            line.setLength(0);
            iCol = 0;
            Iterator<Column> icol = columns.iterator();
            for (String v : row) {
                Column col = icol.next ();
                String f = col.format();
                line.append(" " + f + print(v,col.width,col.leftAlign?0:2) + (f.equals("")?"":col.reset()) + " ");
                if (iCol < nCols - 1)
                    line.append("|");
                iCol ++;
            }
            printStream.println(prefix + line);
        }

        closed = true;
    }

    public void drop () {
        if (closed)
            throw new RuntimeException("already closed");
        closed = true;
    }

    private String print(String s, int width, int align) {
        if (s.length() == width)
            return s;
        if (s.length() > width)
            return s.substring(0,width -1) + "~";
        int rem = width - s.length();
        if (align == 0)
            return s + mult(" ",rem);
        else if (align == 1)
            return mult(" ",rem/2) + s + mult(" ",rem - rem/2);
        else
            return mult(" ",rem) + s;
    }

    private static String mult(String s, int n) {
        StringBuilder res = new StringBuilder();
        for (int ii = 0; ii < n; ii ++)
            res.append(s);
        return res.toString();
    }
}
