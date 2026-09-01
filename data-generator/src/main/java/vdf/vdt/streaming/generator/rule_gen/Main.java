package vdf.vdt.streaming.generator.rule_gen;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        int reqPerSecond = 10;
        int idRange      = 100;
        int totalRules   = 1000;
        // user_id in rule metadata is randomly drawn from "user_001" to "user_<maxUserId>"
        int maxUserId    = 20;
        // Maximum depth for condition_tree AST (tree depth is randomized between 1 and maxTreeDepth per rule)
        int maxTreeDepth = 2;

        // Load schema.version from application.properties so both trigger sources share the same version.
        String schemaVersion = loadSchemaVersion();
        System.out.println("Schema version from config: " + schemaVersion);

        // Base data folder
        Path basePath = Path.of("data/rules").toAbsolutePath();
        System.out.println("Base path: " + basePath);
        String basePathString = basePath.toString();

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss-SSS");
        String timeString = dtf.format(LocalDateTime.now());

        // Eg: data/rules/1000/2026-06-07_14-30-15.json
        String filePath = basePathString + File.separator + totalRules + File.separator + timeString + ".json";

        RuleGenerator ruleService = new RuleGenerator(idRange, reqPerSecond, schemaVersion);
        try {
            Path targetPath = Path.of(filePath);
            if (targetPath.getParent() != null) {
                Files.createDirectories(targetPath.getParent());
            }

            ruleService.generateRulesToFile(totalRules, filePath, maxUserId, maxTreeDepth);
            System.out.println(">>> Rule successfully generated at: " + new File(filePath).getAbsolutePath());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Reads schema.version from the classpath resource application.properties.
    // Falls back to "v2" if the property is missing or the file cannot be loaded.
    private static String loadSchemaVersion() {
        Properties props = new Properties();
        try (InputStream is = Main.class.getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (is != null) {
                props.load(is);
            }
        } catch (IOException e) {
            System.err.println("Warning: could not load application.properties — defaulting schema.version to v2");
        }
        return props.getProperty("schema.version", "v2");
    }
}