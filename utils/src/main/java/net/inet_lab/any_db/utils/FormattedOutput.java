package net.inet_lab.any_db.utils;

import java.util.Arrays;
import java.util.List;

import java.io.PrintStream;
import java.io.IOException;
import java.io.FileOutputStream;

public abstract class FormattedOutput {

    public static FormattedOutput newTabularOutput(PrintStream printStream, int termWidth, int minMaxWidth, String md_flavor) throws IOException {
        if ("jira".equals(md_flavor)) {
            return new MarkdownOutput(printStream, false);
        }
        else {
            return new TabularOutput(printStream, termWidth, minMaxWidth, md_flavor);
        }
    }

    public static CSVOutput newCSVOutput(PrintStream printStream, boolean tuplesOnly) throws IOException {
        return new CSVOutput(printStream, tuplesOnly);
    }

    public static ExcelOutput newExcelOutput(FileOutputStream fileStream) throws IOException {
        return new ExcelOutput(fileStream);
    }

    public static TabbedOutput newTabbedOutput(PrintStream printStream,
                                               boolean tuplesOnly, char separator) throws IOException {
        return new TabbedOutput(printStream, tuplesOnly, separator);
    }

    public static MarkdownOutput newMarkdownOutput(PrintStream printStream,
                                               boolean tuplesOnly) throws IOException {
        return new MarkdownOutput(printStream, tuplesOnly);
    }

    public void addColumn(String header, int width, boolean leftAlign) {
        addColumn(header, width, leftAlign, null, null);
    }

    public abstract void addColumn(String header, int width, boolean leftAlign, String formatPattern,
                                   String[] fmtopts);

    public abstract void writerow(List<String> row) throws IOException;

    public void writerow(String... cols) throws IOException {
        writerow(Arrays.asList(cols));
    }

    public void writerow_excel(List<Object> row) throws IOException {
        throw new RuntimeException("This is Excel mode-specific utility");
    }

    public abstract void close () throws IOException;

    public void drop () throws IOException {
        close ();
    }
}
