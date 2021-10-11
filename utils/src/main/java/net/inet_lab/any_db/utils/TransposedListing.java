package net.inet_lab.any_db.utils;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TransposedListing {
    private static final Logger log = Logger.getLogger(TransposedListing.class.getName());

    public static void run(ResultSet res,
                    String nullString,
                    String keyColName,
                    int limit,
                    int termWidth,
                    int minWidth,
                    int colWidth,
                    String outputFile) throws IOException, SQLException {

        ResultSetMetaData rsmd = res.getMetaData();
        int nCols = rsmd.getColumnCount();
        List<CFormatter> formatters = new ArrayList<>();
        List<String> labels = new ArrayList<>();
        int keyColIdx = -1;
        for (int iCol = 1; iCol <= nCols; iCol++) {
            CFormatter cf = CFormatter.create(rsmd, iCol, nullString);
            String label = rsmd.getColumnLabel(iCol).toLowerCase();
            int idx = label.indexOf('.');
            if (idx < label.length() - 2)
                label = label.substring(idx + 1, label.length());
            formatters.add(cf);
            labels.add(label);
            if (label.equalsIgnoreCase(keyColName))
                keyColIdx = iCol - 1;
        }

        List<List<String>> rows = new ArrayList<>();
        int nRows = 0;
        while (res.next()) {
            List<String> row = new ArrayList<>();
            nRows++;
            for (CFormatter cf : formatters)
                row.add(cf.format(res));
            rows.add(row);
        }

        log.info(String.format("Retrieved %d columns and %d rows%s",
                nCols, nRows,
                (nRows > limit) ? " (over the limit)" : ""));
        if (nRows == 0) {
            return;
        }

        FormattedOutput out;
        if (outputFile == null)
            out = FormattedOutput.newTabularOutput(System.out, termWidth, minWidth, null);
        else
            out = FormattedOutput.newCSVOutput(new PrintStream(outputFile, StandardCharsets.UTF_8.name()), false);

        if (nRows > limit)
            nRows = limit;

        out.addColumn("column", 0, true);
        for (int ii = 0; ii < nRows; ii++)
            out.addColumn((keyColIdx < 0) ? ("<" + (ii + 1) + ">") : rows.get(ii).get(keyColIdx), colWidth, false);

        for (int ii = 0; ii < nCols; ii++) {
            List<String> row = new ArrayList<>();

            row.add(labels.get(ii));
            for (int jj = 0; jj < nRows; jj++)
                row.add(rows.get(jj).get(ii));
            out.writerow(row);
        }

        out.close();
    }

}
