package net.inet_lab.any_db.utils;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.log4j.Logger;

import net.inet_lab.any_db.utils.FormattedOutput;

public class MarkupOutput extends FormattedOutput {
    private static final Logger log = Logger.getLogger(MarkupOutput.class.getName());

    private final List<String> columns;
    private boolean doneWithHeader;
    private final boolean tuplesOnly;
    private final PrintStream printStream;

    public MarkupOutput(PrintStream printStream, boolean tuplesOnly) throws IOException {
        this.tuplesOnly = tuplesOnly;
        this.printStream = printStream;
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
                printRow(columns,"||");
        }
        printRow(row,"|");
    }

    public void close () throws IOException {
    }

    private void printRow(List<String> row, String separator) throws IOException {
        boolean first = true;
        printStream.print(separator);
        for(String col: row) {
            printStream.print(col);
            printStream.print(separator);
        }
        printStream.print('\n');
    }
}
