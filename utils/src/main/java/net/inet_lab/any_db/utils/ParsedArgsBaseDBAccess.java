package net.inet_lab.any_db.utils;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class ParsedArgsBaseDBAccess extends ParsedArgsBase {
    public DBConfig getConfig(final List<String> freeArgs, final Map<String,String> vars) {
        DBConfig knownConfig = null;
        Pattern varsub = Pattern.compile("^([a-zA-Z0-9_]+)=(.*)$");
        for (String loArg : parsedCL.getArgs()) {
            if (vars != null) {
                Matcher m = varsub.matcher(loArg);
                if (m.matches()) {
                    vars.put(m.group(1), m.group(2));
                    continue;
                }
            }
            if (knownConfig == null) {
                knownConfig = DBConfig.valueOf(loArg);
                if (knownConfig != null) {
                    continue;
                }
            }
            freeArgs.add(loArg);
        }
        if (knownConfig == null) {
            String defaultConf = optStringVal("default-config");
            if (defaultConf != null) {
                knownConfig = DBConfig.valueOf(defaultConf);
                if (knownConfig == null) {
                    throw new RuntimeException("Default config value '" + defaultConf + "' is invalid");
                }
            }
        }

        return DBConfig.make(knownConfig, parsedCL.getOptionValue("driver"),
                parsedCL.getOptionValue("host"), optIntegerVal("port"),
                parsedCL.getOptionValue("database"));
    }

    public boolean verifyConfig(DBConfig dbConfig) {
        int err = 0;
        if (dbConfig.getHost() == null) {
            System.err.println("Missing host (use -h,--host)");
            err += 1;
        }
//        if (dbConfig.getPort() == null) {
//            System.err.println("Missing port (use -p,--port)");
//            err += 1;
//        }
        if (dbConfig.getDatabase() == null) {
            System.err.println("Missing host (use -d,--database)");
            err += 1;
        }
        return err == 0;
    }
}
