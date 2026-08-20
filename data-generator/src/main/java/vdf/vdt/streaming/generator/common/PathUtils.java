package vdf.vdt.streaming.generator.common;

import java.nio.file.Path;
import java.nio.file.Paths;

public class PathUtils {

    public static Path getParentPath(int levelsUp) {
        if (levelsUp < 0) {
            throw new IllegalArgumentException("Levels up must be greater than or equal to 0");
        }

        Path currentPath = Paths.get("").toAbsolutePath();
        for (int i = 0; i < levelsUp; i++) {
            if (currentPath != null) {
                currentPath = currentPath.getParent();
            } else {
                break;
            }
        }
        return currentPath;
    }
}