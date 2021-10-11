package net.inet_lab.any_db.utils;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import org.jline.terminal.TerminalBuilder;

public abstract class ParsedArgsBase {
    private static final Logger log = Logger.getLogger(ParsedArgsBase.class.getName());

    protected CommandLine parsedCL;
    protected String restoreCL;
    private Integer termWidth;

    protected CommandLine parseCL(Options options, String[] commandLine) {
        StringBuilder sb = new StringBuilder();
        for (String cla: commandLine) {
            String q;
            if (cla.indexOf(' ')<0 && cla.indexOf('"')<0 && cla.indexOf('\'')<0 && cla.indexOf('$')<0)
                q = "";
            else if (cla.indexOf('\'')<0)
                q = "'";
            else {
                cla = cla.replaceAll("\\$","\\\\\\$").replaceAll("\"","\\\\\"");
                q = "\"";
            }
            if (sb.length() > 0)
                sb.append(' ');
            sb.append(q + cla + q);
        }
        restoreCL = sb.toString ();
        final CommandLineParser parser = new DefaultParser();
        try {
            return parser.parse(options,commandLine);
        }
        catch (ParseException exp) {
            System.err.println ("Parsing failed.  Reason: " + exp.getMessage());
            System.exit(1);
            return null;
        }
    }

    public int getTermWidth()  {
        if (termWidth == null) {
            try {
                termWidth = TerminalBuilder.builder().build().getWidth();
                log.debug("Console width = " + termWidth);
            } catch (Exception e) {
                log.warn("Can't determine terminal width: " + e);
                termWidth = 80;
            }

        }
        return termWidth;
    }

    protected String optStringVal(String name, String deflt) {
        String val = parsedCL.getOptionValue( name );
        if (val == null)
            return deflt;
        else
            return val;
    }

    protected String optStringVal(String name) {
        return optStringVal(name, null);
    }

    protected Boolean optBooleanVal(String name) {
        String val = parsedCL.getOptionValue( name );
        if (val == null)
            return null;
        else if (val.equalsIgnoreCase("y") ||
                 val.equalsIgnoreCase("yes") ||
                 val.equalsIgnoreCase("true") ||
                 val.equalsIgnoreCase("t"))
            return true;
        else if (val.equalsIgnoreCase("n") ||
                 val.equalsIgnoreCase("no") ||
                 val.equalsIgnoreCase("false") ||
                 val.equalsIgnoreCase("f"))
            return false;
        else {
            System.err.println("Invalid value " + val + " for option --" + name + ", should be YES or NO");
            System.exit(1);
            return null;
        }
    }

    protected Integer optIntegerVal(String name) {
        return optIntegerVal(name, null);
    }

    protected Integer optIntegerVal(String name, Integer deflt) {
        String val = parsedCL.getOptionValue( name );
        if (val == null)
            return deflt;
        try {
            return Integer.parseInt(val);
        }
        catch (NumberFormatException err) {
            System.err.println("Invalid value " + val + " for option --" + name + ", expecting INTEGER");
            System.exit(1);
            return null;
        }
    }
}
