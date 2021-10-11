package net.inet_lab.any_db.sqli;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Collection;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.sql.SQLException;
import java.rmi.RemoteException;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;

import net.inet_lab.any_db.utils.SQLConnection;
import static net.inet_lab.any_db.utils.Misc.*;

// Example:
// github.com/jline/jline3/blob/master/reader/src/main/java/org/jline/reader/impl/completer/FileNameCompleter.java

public class SQLCompleter implements Completer {
    private static final Logger log = Logger.getLogger(SQLCompleter.class.getName());

    private final SQLConnection conn;
    private final SQLDict dict;
    private final SQLiContext sqlCtx;

    private final static String CTX_COMMAND = "command";
    private final static String CTX_TABLE = "table";
    private final static String CTX_SQL = "sql";
    private final static String CTX_COLUMN = "column";

    SQLCompleter (SQLConnection conn, SQLDict dict, SQLiContext sqlCtx) {
        this.conn = conn;
        this.dict = dict;
        this.sqlCtx = sqlCtx;
    }

    @Override
    public void complete(LineReader lineReader, ParsedLine parsedLine, List<Candidate> list) {
        //public int complete(final String buffer, final int cursor, final List<CharSequence> candidates) {
        int pos = -1;
        long t0 = System.currentTimeMillis();
        List<String> candidates = new ArrayList<>();

        try {
            pos = complete_low(lineReader, parsedLine, candidates);
            log.info(String.format("complete_low() returned %d, yielded %d results, took %.3f secs",
                    pos, candidates.size(), (System.currentTimeMillis() - t0) / 1000.0));

            final String word = parsedLine.word();
            log.info("Word = " + word);
            final int wcursor = parsedLine.wordCursor();
            final int cursor = parsedLine.cursor();
            int cut = cursor - wcursor - pos;

            if(pos < 0)
                log.debug("No candidates, nothing to do in terms of adjustments");
            else if (cut > 0)
                log.debug("returned pos = " + pos + " is behind expected " + cursor + " - " + wcursor + "; cutting strings by " + cut + " char(s)");
            else if (cut < 0)
                log.debug("returned pos = " + pos + " is in front of expected " + cursor + " - " + wcursor + "; prepending word " +
                        word.substring(0,-cut) + " to candidates");
            else
                log.debug("Candidated correctly aligned, need no adjsustment");

            for (String cand : candidates) {
                list.add(new Candidate(
                        (cut < 0)?(word.substring(0,-cut) + cand):(
                        (cut > 0)?(cand.substring(cut))
                                 :cand
                )));
            }

        } catch (SQLException err) {
            log.error(ExceptionUtils.getRootCauseMessage(err));
        } catch (IOException err) {
            err.printStackTrace();
        }

        StringBuilder r = new StringBuilder();
        for (Candidate c: list) {
            if (r.length() > 0)
                r.append(", ");
            r.append(c.value());
        }
        log.debug("complete() returning " + list.size() + " results: " + r);
    }

    private int complete_low(final LineReader lineReader, final ParsedLine parsedLine, final List<String> candidates) throws SQLException, IOException {
        List<String> strings = new ArrayList<>();
        final int cursor = parsedLine.cursor();
        final String buffer = parsedLine.line();

        if (buffer == null || buffer.length() == 0) {
            candidates.add("\\help");
            return 0;
        }

        final String activeSql = buffer.substring(0, cursor);
        String activeWord = getActiveWord(activeSql);
        if (activeWord == null)
            return -1;
        String ctx = getContext(activeSql);
        int pos;

        log.debug("Active word = " + activeWord + ", context = " + ctx);

        if (CTX_COMMAND.equals(ctx)) {
            for (SQLiCommand.Command cmd : SQLiCommand.Command.values()) {
                for (String arg: cmd.args)
                    strings.add("\\" + arg);
            }

            pos = 0;

        }
        else if (CTX_TABLE.equals(ctx)) {
            int idx = activeWord.lastIndexOf('.');
            if (idx == -1) {
                try {
                    for (SQLDict.Schema schema : dict.getSchemas(true))
                        strings.add(schema.name + ".");
                } catch (SQLDict.Error err) {
                    log.error(err);
                }
                try {
                    for (SQLDict.Table table : dict.getTables(sqlCtx.getSchema(), true))
                        strings.add(table.name);
                } catch (SQLDict.Error err) {
                    log.error(err);
                }
                pos = cursor - activeWord.length();
            }
            else {
                Collection<SQLDict.Table> tables = null;
                try {
                    tables = dict.getTables(activeWord.substring(0, idx), true);
                } catch (SQLDict.Error err) {
                    log.error(err);
                }
                for (SQLDict.Table table : tables)
                    strings.add(table.name);
                pos = cursor - activeWord.length() + idx + 1;
            }
        }
        else if (CTX_COLUMN.equals(ctx)) {
            int idx = activeWord.lastIndexOf('.');
            pos = cursor - activeWord.length() + idx + 1; // ok also if idx = -1

            Set<String> tables = extractTables(buffer);
            if (sqlCtx.getLastCheckedTable() != null)
                tables.add(sqlCtx.getLastCheckedTable());

            if (tables.isEmpty()) {
                log.debug("No tables available for column completion");
                return -1;
            }

            log.debug("Using tables to complete: " + join(", ", tables));
            Set<String> cand_s = new HashSet<>();
            for (String table: tables) {
                try {
                    for (SQLDict.Column col : dict.getColumns(table, sqlCtx.getSchema(), true))
                        cand_s.add(col.name);
                } catch (SQLDict.Error err) {
                    log.error(err);;
                }
            }

            strings.addAll(cand_s);
        }
        else if (CTX_SQL.equals(ctx)) {
            strings.addAll(Arrays.asList("select", "with", "create", "drop", "desc", "show"));
            pos = cursor - activeWord.length();
        }
        else {
            log.info("Context " + ctx + " not implemented");
            return -1;
        }

        for (String cand : strings) {
            if (cand.startsWith(activeSql.substring(pos,cursor)))
                candidates.add(cand);
        }

        if (candidates.isEmpty())
            return -1;

        return pos;
    }

    private String getContext(String sql) {
        //log.debug("getContext(" + sql + ")");
        Pattern p = Pattern.compile("\\\\[^ \t]*$");
        Matcher m = p.matcher(sql);

        if (m.lookingAt())
            return CTX_COMMAND;

        if(sql.startsWith("\\"))
            return CTX_TABLE;

        p = Pattern.compile(".+(from|join)\\s+[a-z0-9_.]+$", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        m = p.matcher(sql);

        if (m.lookingAt())
            return CTX_TABLE;

        if (Pattern.compile("[a-z]+$").matcher(sql).lookingAt())
            return CTX_SQL;

        return CTX_COLUMN;
    }

    private String getActiveWord(String sql) {
        Pattern p = Pattern.compile(".*?([0-9a-z_\\\\.]+)$", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher m = p.matcher(sql);

        if (m.lookingAt())
            return m.group(1);
        else
            return null;
    }

    private Set<String> extractTables(String buffer) {
        Set<String> tables = new HashSet<>();
        Matcher m = Pattern.compile("(from|join)\\s+([a-z0-9_.]+)", Pattern.CASE_INSENSITIVE)
                .matcher(buffer);
        while (m.find()) {
            tables.add(m.group(2));
        }
        return tables;
    }

}
