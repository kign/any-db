package net.inet_lab.any_db.utils;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.IOException;
import java.io.FileOutputStream;

import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.ss.usermodel.PrintSetup;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.HorizontalAlignment;

//import org.apache.poi.ss.usermodel.CellType;

import org.apache.log4j.Logger;

import net.inet_lab.any_db.utils.FormattedOutput;

public class ExcelOutput extends FormattedOutput {
    private static final Logger log = Logger.getLogger(ExcelOutput.class.getName());
    private boolean doneWithHeader;
    private final FileOutputStream fileStream;
    private final List<Column> columns;

    private final XSSFWorkbook workbook;
    private final XSSFSheet sheet;
    private int rIdx;

    private class Column {
        public final String[] fmtopts;
        public final String   header;
        public final boolean  leftAlign;
        public final String   formatPattern;

        public final DataFormat dataFormat;
        public final CellStyle cellStyle;

        public Column(String header, boolean leftAlign, String formatPattern, String[] fmtopts) {
            this.header = header;
            this.leftAlign = leftAlign;
            this.fmtopts = fmtopts;
            this.formatPattern = formatPattern;

            dataFormat = workbook.createDataFormat();
            cellStyle = workbook.createCellStyle();
            cellStyle.setAlignment(leftAlign? CellStyle.ALIGN_LEFT : CellStyle.ALIGN_RIGHT);
            if (formatPattern != null) {
                cellStyle.setDataFormat(dataFormat.getFormat(formatPattern));
            }
        }
    }

    public ExcelOutput(FileOutputStream fileStream) throws IOException {
        this.fileStream = fileStream;
        workbook = new XSSFWorkbook();
        sheet = workbook.createSheet("data");
        doneWithHeader = false;
        columns = new ArrayList<>();
        rIdx = 0;

        //turn off gridlines
        //sheet.setDisplayGridlines(false);
        //sheet.setPrintGridlines(false);
        sheet.setFitToPage(true);
        sheet.setHorizontallyCenter(true);
        PrintSetup printSetup = sheet.getPrintSetup();
        printSetup.setLandscape(true);

        // Not available in 3.10, probably in 3.14
        // sheet.addIgnoredErrors(.....);
    }

    public void addColumn(String header, int width, boolean leftAlign, String formatPattern, String[] fmtopts) {
        if (doneWithHeader)
            throw new RuntimeException("Cannot call addColumn(" + header + ") at this point");
        columns.add(new Column(header, leftAlign, formatPattern, fmtopts));
    }

    public void writerow(List<String> rowValues) throws IOException {
        List<Object> objVals = new ArrayList<>();
        for (String cellValue : rowValues)
            objVals.add((Object)cellValue);
        writerow_excel(objVals);
    }

    public void writerow_excel(List<Object> rowValues) throws IOException {
         if (!doneWithHeader) {
            doneWithHeader = true;

            Row headerRow = sheet.createRow(rIdx ++);
            int cIdx = -1;
            for(Column col : columns) {
                cIdx ++;
                Cell cell = headerRow.createCell(cIdx);
                cell.setCellValue(col.header);
            }
            //freeze the first row
            sheet.createFreezePane(0, 1);
         }

         Row row = sheet.createRow(rIdx ++);

         Iterator<Object> valIter = rowValues.iterator();
         Iterator<Column> colIter = columns.iterator();
         for (int cIdx = 0; valIter.hasNext() && colIter.hasNext(); cIdx ++) {

             Cell cell = row.createCell(cIdx);
             Object val = valIter.next ();
             Column col = colIter.next ();

             if (val instanceof Integer)
                 cell.setCellValue((int)val);
             else if (val instanceof Long)
                 cell.setCellValue((long)val);
             else if (val instanceof Boolean)
                 cell.setCellValue((Boolean)val);
             else if (val instanceof Double)
                 cell.setCellValue((Double)val);
             else if (val instanceof String)
                 cell.setCellValue((String)val);
             else
                 throw new RuntimeException("Type " + val.getClass() + " not implemented yet");

             cell.setCellStyle(col.cellStyle);
         }
    }

    public void close () throws IOException {
        workbook.write(fileStream);
    }
}
