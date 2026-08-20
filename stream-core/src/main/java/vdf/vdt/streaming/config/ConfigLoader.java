package vdf.vdt.streaming.config;

import org.yaml.snakeyaml.Yaml;
import java.io.InputStream;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConfigLoader {
    private static Map<String, Object> configCache;

    public static synchronized Map<String, Object> load() {
        if (configCache != null) {
            return configCache;
        }
        try {
            String env = System.getenv().getOrDefault("APP_ENV", "dev");
            String fileName = "config." + env + ".yml";

            String content;
            try (InputStream is = ConfigLoader.class.getClassLoader().getResourceAsStream(fileName)) {
                if (is == null) {
                    throw new RuntimeException("Config not found: " + fileName);
                }
                content = new String(is.readAllBytes());
            }

            // Env
            Matcher matcher = Pattern.compile("\\$\\{(\\w+)}").matcher(content);
            StringBuilder sb = new StringBuilder();
            while (matcher.find()) {
                String val = System.getenv().getOrDefault(matcher.group(1), matcher.group(0));
                matcher.appendReplacement(sb, Matcher.quoteReplacement(val));
            }
            matcher.appendTail(sb);

            Yaml yaml = new Yaml();
            configCache = yaml.load(sb.toString());
            return configCache;
        } catch (Exception e) {
            throw new RuntimeException("Load config failed", e);
        }
    }

    public static void main(String[] args) {
        try {
            System.out.println("Đang thử load cấu hình...");
            Map<String, Object> config = load();

            System.out.println("Load thành công! Nội dung cấu hình:");
            System.out.println(config);

            Map<String, Object> app = (Map<String, Object>) config.get("app");
            System.out.println("App name: " + app.get("name"));

        } catch (Exception e) {
            System.err.println("Test load config thất bại: " + e.getMessage());
            e.printStackTrace();
        }
    }
}