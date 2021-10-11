package net.inet_lab.any_db.utils;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.log4j.Logger;

import net.inet_lab.any_db.utils.FormattedOutput;

public class TabbedOutput extends FormattedOutput {
    private static final Logger log = Logger.getLogger(TabbedOutput.class.getName());

    private final List<String> columns;
    private boolean doneWithHeader;
    private final boolean tuplesOnly;
    private final PrintStream printStream;
    private final char separator;

    public TabbedOutput(PrintStream printStream, boolean tuplesOnly, char separator) throws IOException {
        this.tuplesOnly = tuplesOnly;
        this.printStream = printStream;
        this.separator = separator;
        columns = new ArrayList<>();
    }

    public void addColumn(String header, int width, boolean leftAlign, String formatPattern, String[] fmtopts) {
        if (doneWithHeader)
            throw new RuntimeException("Cannot call addColumn(" + header + ") at this point");
        columns.add(header);
    }
    
    public void writerow(List<String> row) throws IOException {
        if (!doneWithHeader) {
            doneWithHeader = true;
            if (!tuplesOnly)
                printRow(columns);
        }
        printRow(row);
    }

    public void close () throws IOException {
    }

    private void printRow(List<String> row) throws IOException {
        boolean first = true;
        for(String col: row) {
            if (first)
                first = false;
            else
                printStream.print(separator);
            printStream.print(col);
        }
        printStream.print('\n');
    }
}
