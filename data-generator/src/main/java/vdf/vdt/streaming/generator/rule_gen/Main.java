package vdf.vdt.streaming.generator.rule_gen;

import java.nio.file.Files;
import java.nio.file.Path;
import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Main {
    public static void main(String[] args) {
        int reqPerSecond = 10;
        int idRange      = 100;
        int totalRules   = 1000;
        // user_id in rule metadata is randomly drawn from "user_001" to "user_<maxUserId>"
        int maxUserId    = 20;
        // Maximum depth for condition_tree AST (tree depth is randomized between 1 and maxTreeDepth per rule)
        int maxTreeDepth = 2;

        // Base data folder
        Path basePath = Path.of("data/rules").toAbsolutePath();
        System.out.println("Base path: " + basePath);
        String basePathString = basePath.toString();

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss-SSS");
        String timeString = dtf.format(LocalDateTime.now());

        // Eg: rules_output/100/2026-06-07_14-30-15.json
        String filePath = basePathString + File.separator + totalRules + File.separator + timeString + ".json";

        RuleGenerator ruleService = new RuleGenerator(idRange, reqPerSecond);
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
}