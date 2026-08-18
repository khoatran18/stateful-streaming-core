package vdf.vdt.streaming.generator.rule_gen;

import java.io.File;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        Properties props = new Properties();
        try (InputStream input = Main.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input != null) props.load(input);
        } catch (Exception e) {
            e.printStackTrace();
        }

        int totalRules = Integer.parseInt(props.getProperty("rule.default.total-rules", "100"));
        String basePath = props.getProperty("rule.output.base-path", "rules_output");

        // Format thời gian hiện tại đến giây, dùng dấu gạch dưới (_)
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
        String timeString = dtf.format(LocalDateTime.now());

        // Cấu trúc đường dẫn: path/so_rule/time.json (Ví dụ: rules_output/100/2026-06-07_14-30-15.json)
        String filePath = basePath + File.separator + totalRules + File.separator + timeString + ".json";

        RuleGenerator ruleService = new RuleGenerator();
        try {
            ruleService.generateRulesToFile(totalRules, filePath);
            System.out.println(">>> Rule successfully generated at: " + new File(filePath).getAbsolutePath());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}