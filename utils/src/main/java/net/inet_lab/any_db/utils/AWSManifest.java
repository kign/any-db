package net.inet_lab.any_db.utils;

import java.util.List;
import java.util.ArrayList;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class AWSManifest {
    public final List<Entry> entries;

    public AWSManifest() {
        entries = new ArrayList<>();
    }

    public static class Entry {
        private final String url;
        public Entry (String url) {
            this.url = url;
        }
    }
    
    public String toJson()
    {
        Gson gson = new GsonBuilder()
                .setPrettyPrinting()
                .create();
        return gson.toJson(this);
    }    

}
