package net.inet_lab.any_db.utils;

import java.util.HashMap;
import java.util.Map;


public class DBUserConfig {
    static public String getSnowflakeRole(boolean is_admin, String user) {
        Map<String,String[]> roles = new HashMap<>();
        // FIXME: use proper configuration
        roles.put("WHAM_ROLE", new String[]{"my_user_name"});

        if (is_admin)
            return "SYSADMIN";
        if (user.indexOf('_') >= 0)
            return user.toUpperCase() + "_ROLE";
        for (String role: roles.keySet()) {
            for (String u: roles.get(role))
                if (user.equalsIgnoreCase(u))
                    return role;
        }
        return null;
    }
}
