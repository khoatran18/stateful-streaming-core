package vdf.vdt.streaming.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// Singleton config loader. Reads config.{APP_ENV}.yml from the classpath,
// resolves ${VAR} placeholders from environment variables, and caches the result.
public class ConfigLoader {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigLoader.class);

    private static Map<String, Object> configCache;

    // Loads and caches the config map. Thread-safe (synchronized).
    // File selected by APP_ENV env var (default: "dev" → config.dev.yml).
    public static synchronized Map<String, Object> load() {
        if (configCache != null) {
            LOG.debug("Returning cached config");
            return configCache;
        }

        String env = System.getenv().getOrDefault("APP_ENV", "dev");
        String fileName = "config." + env + ".yml";
        LOG.info("Loading config file: {}", fileName);

        try {
            String content;
            try (InputStream is = ConfigLoader.class.getClassLoader().getResourceAsStream(fileName)) {
                if (is == null) {
                    throw new RuntimeException("Config file not found on classpath: " + fileName);
                }
                content = new String(is.readAllBytes());
            }

            // Resolve ${VAR} placeholders from environment variables
            Matcher matcher = Pattern.compile("\\$\\{(\\w+)}").matcher(content);
            StringBuilder sb = new StringBuilder();
            while (matcher.find()) {
                String varName = matcher.group(1);
                String val = System.getenv().getOrDefault(varName, matcher.group(0));
                if (val.equals(matcher.group(0))) {
                    LOG.warn("Env var '{}' not set, keeping placeholder as-is", varName);
                }
                matcher.appendReplacement(sb, Matcher.quoteReplacement(val));
            }
            matcher.appendTail(sb);

            Yaml yaml = new Yaml();
            configCache = yaml.load(sb.toString());

            LOG.info("Config loaded from '{}' with {} top-level keys", fileName, configCache.size());
            return configCache;

        } catch (Exception e) {
            LOG.error("Failed to load config file: {}", fileName, e);
            throw new RuntimeException("Load config failed", e);
        }
    }
}