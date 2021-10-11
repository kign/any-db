package net.inet_lab.any_db.sqli;

import org.apache.log4j.Logger;
import org.jline.reader.EOFError;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.DefaultParser;

class SQLiParser extends DefaultParser {
    private static final Logger log = Logger.getLogger(SQLiParser.class.getName());

    SQLiParser() {
        super();
        setEscapeChars(null);
        //setQuoteChars(null);
    }

    public ParsedLine parse(final String line, final int cursor, ParseContext context) {
        //log.debug("parse(" + line + ", " + cursor + ", " + context + ")");
        if (context == ParseContext.ACCEPT_LINE  && !(line.endsWith(";") || line.startsWith("\\") || line.trim().length() == 0))
            throw new EOFError(0, 0, null, null);
        else
            return super.parse(line, cursor, context);

    }
}
