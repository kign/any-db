package net.inet_lab.any_db.utils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;

public class Misc {
    @SafeVarargs
    public static <T> T coalesce(T ...items) {
        for(T i : items) if(i != null) return i;
        return null;
    }

    public static <T> T getArg(T[] args, int idx, T deflt) {
        if (idx < args.length)
            return args[idx];
        return deflt;
    }

    public static <T,K> K accessMap(Map<T,K> map, T key, K deflt) {
        if (map.containsKey(key))
            return map.get(key);
        return deflt;
    }

    public static String join(String sep, String... args) {
        return join(sep, Arrays.asList(args));
    }

    public static String join(String sep, Collection<String> args) {
        StringBuilder b = new StringBuilder ();
        int idx = 0;
        for (String arg : args) {
            if (arg != null) {
                idx++;
                if (idx > 1)
                    b.append(sep);
                b.append(arg);
            }
        }
        return b.toString();
    }

    public interface ParametersAccess {
        String get(String par);
    }
    public static class ParametersError extends Exception {
        ParametersError(String msg) {
            super(msg);
        }
    }
    public static String substituteParameters(String code, ParametersAccess pars) throws ParametersError {
        String[] subPatterns = {"\\$\\{(?:[a-zA-Z0-9_]+:)?([a-zA-z0-9_]+)\\}", "(?<!:):([a-zA-Z][a-zA-Z0-9_]*)\\b"};
        Set<String> non_matches = new HashSet<>();
        for (String subPattern : subPatterns) {
            Pattern p = Pattern.compile(subPattern);
            Matcher m = p.matcher(code);
            StringBuffer sb = new StringBuffer();
            while (m.find()) {
                String val = pars.get(m.group(1));
                if (val != null)
                    m.appendReplacement(sb, val);
                else
                    non_matches.add(m.group(0));
            }
            m.appendTail(sb);
            code = sb.toString();
        }
        if (!non_matches.isEmpty()) {
            throw new ParametersError("Pattern(s) not defined " + join(", ", non_matches));
        }
        return code;
    }

    static public String pipe(String input, String[] cmd) {
        try {
            Runtime rt = Runtime.getRuntime();
            Process proc = rt.exec(cmd);
            proc.getOutputStream().write(input.getBytes());
            proc.getOutputStream().flush();
            proc.getOutputStream().close();
            String output = IOUtils.toString(proc.getInputStream());
            proc.waitFor();
            return output;
        } catch (java.io.IOException | InterruptedException ex) {
            System.err.println(cmd[0] + " not available");
            return null;
        }
    }

    static public void pipe_to_stdout(String input, String[] cmd) {
        try {
            Runtime rt = Runtime.getRuntime();
            ProcessBuilder pb = new ProcessBuilder(Arrays.asList(cmd));
            pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
            pb.redirectError(ProcessBuilder.Redirect.INHERIT);
            Process proc = pb.start();
            proc.getOutputStream().write(input.getBytes());
            proc.getOutputStream().flush();
            proc.getOutputStream().close();
            proc.waitFor();
        } catch (java.io.IOException | InterruptedException ex) {
            System.out.println(input);
        }
    }

    static public String beautifySql(String sql) {
        return coalesce(pipe(sql, new String[]{"sqlformat", "-", "-r"}), sql);
    }

    static public String straightenSql(String sql) {
        ArrayList<String> s_a = new ArrayList<>();
        ScriptUtils.splitSqlScript(sql, s_a);
        for (int ii = 0; ii < s_a.size(); ii ++)
            s_a.set(ii, s_a.get(ii).replaceAll("\\n", " ").replaceAll("\\s+", " "));
        return join("\n",s_a);
    }

    static public String[] splitArgs(String cmd) throws IOException {
        final List<String> res = new ArrayList<>();
        int N = cmd.length();

        for(int ii=0; ii < N; ii++) {
            char c = cmd.charAt(ii);
            while (ii < N && Character.isWhitespace(cmd.charAt(ii)))
                ii ++;
            if (ii >= N)
                break;
            StringBuffer arg = new StringBuffer();
            if (c == '"') {
                int ii0 = ii;
                ii ++;
                while (ii < N && cmd.charAt(ii) != '"') {
                    c = cmd.charAt(ii);
                    if (c == '\\' && ii+1 < N) {
                        char c1 = cmd.charAt(ii + 1);
                        if (c1 == '"' || c1 == '\\') {
                            c = c1;
                            ii ++;
                        }
                    }
                    arg.append(c);
                    ii ++;
                }
                if (ii == N)
                    throw new IOException("Cannot find matching quote to position " + (1 + ii0));
                else {
                    res.add(arg.toString());
                    ii ++;
                }
            }
            else {
                while (ii < N && !Character.isWhitespace(cmd.charAt(ii))) {
                    c = cmd.charAt(ii);
                    if (c == '\\' && ii+1 < N) {
                        char c1 = cmd.charAt(ii + 1);
                        if (c1 == '"' || c1 == '\\') {
                            c = c1;
                            ii ++;
                        }
                    }
                    arg.append(c);
                    ii ++;
                }
                res.add(arg.toString());
            }
        }

        return res.toArray(new String[0]);
    }
}
