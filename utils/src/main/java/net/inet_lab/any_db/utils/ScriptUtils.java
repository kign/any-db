package net.inet_lab.any_db.utils;

import java.util.List;

// https://github.com/spring-projects/spring-framework/blob/master/spring-jdbc/src/main/java/org/springframework/jdbc/datasource/init/ScriptUtils.java

public abstract class ScriptUtils {
    public static final String DEFAULT_STATEMENT_SEPARATOR = ";";
    public static final String FALLBACK_STATEMENT_SEPARATOR = "\n";
    public static final String EOF_STATEMENT_SEPARATOR = "^^^ END OF SCRIPT ^^^";
    public static final String DEFAULT_COMMENT_PREFIX = "--";
    public static final String DEFAULT_BLOCK_COMMENT_START_DELIMITER = "/*";
    public static final String DEFAULT_BLOCK_COMMENT_END_DELIMITER = "*/";
    public static final boolean compressMode = false;

    public static void splitSqlScript(String script, List<String> statements) {
        splitSqlScript(script, DEFAULT_STATEMENT_SEPARATOR, statements);
    }
    
    public static void splitSqlScript(String script, char separator, List<String> statements) {
        splitSqlScript(script, String.valueOf(separator), statements);
    }

    public static void splitSqlScript(String script, String separator, List<String> statements)  {
        splitSqlScript(script, separator, DEFAULT_COMMENT_PREFIX, DEFAULT_BLOCK_COMMENT_START_DELIMITER,
                DEFAULT_BLOCK_COMMENT_END_DELIMITER, statements);
    }

    public static void splitSqlScript(String script, String separator, String commentPrefix,
             String blockCommentStartDelimiter, String blockCommentEndDelimiter, List<String> statements) {

        StringBuilder sb = new StringBuilder();
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean inEscape = false;
        char[] content = script.toCharArray();
        for (int i = 0; i < script.length(); i++) {
            char c = content[i];
            if (inEscape) {
                inEscape = false;
                sb.append(c);
                continue;
            }
            // MySQL style escapes
            if (c == '\\') {
                inEscape = true;
                sb.append(c);
                continue;
            }
            if (!inDoubleQuote && (c == '\'')) {
                inSingleQuote = !inSingleQuote;
            }
            else if (!inSingleQuote && (c == '"')) {
                inDoubleQuote = !inDoubleQuote;
            }
            if (!inSingleQuote && !inDoubleQuote) {
                if (script.startsWith(separator, i)) {
                    // we've reached the end of the current statement
                    if (sb.length() > 0) {
                        statements.add(sb.toString().trim());
                        sb = new StringBuilder();
                    }
                    i += separator.length() - 1;
                    continue;
                }
                else if (script.startsWith(commentPrefix, i)) {
                    // skip over any content from the start of the comment to the EOL
                    int indexOfNextNewline = script.indexOf("\n", i);
                    if (indexOfNextNewline > i) {
                        i = indexOfNextNewline;
                        continue;
                    }
                    else {
                        // if there's no EOL, we must be at the end
                        // of the script, so stop here.
                        break;
                    }
                }
                else if (script.startsWith(blockCommentStartDelimiter, i)) {
                    // skip over any block comments
                    int indexOfCommentEnd = script.indexOf(blockCommentEndDelimiter, i);
                    if (indexOfCommentEnd > i) {
                        i = indexOfCommentEnd + blockCommentEndDelimiter.length() - 1;
                        continue;
                    }
                    else {
                        throw new RuntimeException("Missing block comment end delimiter " + 
                                                   blockCommentEndDelimiter);
                    }
                }
                else if (compressMode && (c == ' ' || c == '\n' || c == '\t')) {
                    // avoid multiple adjacent whitespace characters
                    if (sb.length() > 0 && sb.charAt(sb.length() - 1) != ' ') {
                    }
                    else {
                        continue;
                    }
                }
            }
            sb.append(c);
        }
        if (sb.toString().trim().length() > 0) {
            statements.add(sb.toString().trim());
        }
    }
}
