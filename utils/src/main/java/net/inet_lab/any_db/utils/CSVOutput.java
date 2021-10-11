package net.inet_lab.any_db.utils;

import java.util.List;
import java.util.ArrayList;
import java.io.PrintStream;
import java.io.IOException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.log4j.Logger;

public class CSVOutput extends FormattedOutput {
    private static final Logger log = Logger.getLogger(CSVOutput.class.getName());

    private final CSVPrinter csvWriter;
    private final List<String> columns;
    private boolean doneWithHeader;
    private final boolean tuplesOnly;

    public CSVOutput(PrintStream printStream, boolean tuplesOnly) throws IOException {
        csvWriter = CSVFormat.DEFAULT.print(printStream);
        columns = new ArrayList<>();
        doneWithHeader = false;
        this.tuplesOnly = tuplesOnly;
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
                csvWriter.printRecord(columns);
        }
        csvWriter.printRecord(row);
    }
    
    public void close () throws IOException {
        csvWriter.close ();
    }
}
        
