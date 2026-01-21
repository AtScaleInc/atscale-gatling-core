package com.atscale.java.utils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RunLogUtils {
    private static final Pattern RUN_ID = Pattern.compile("gatlingRunId='([^']+)'");

    /** Return unique run ids preserving first-seen order. */
    public static List<String> extractGatlingRunIds(Path filePath){
        File file = filePath.toFile();
        LinkedHashSet<String> ids = new LinkedHashSet<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                Matcher m = RUN_ID.matcher(line);
                while (m.find()) {
                    ids.add(m.group(1));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Error reading file: " + filePath, e);
        }
        return new ArrayList<>(ids);
    }
}
