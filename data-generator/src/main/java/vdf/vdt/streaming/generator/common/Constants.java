package vdf.vdt.streaming.generator.common;

import java.util.List;
import java.util.Map;

public class Constants {
    // Tên các trường định danh và số cho từng schema (đảm bảo tên trường khác nhau hoàn toàn)
    public static final Map<Integer, List<String>> CATEGORICAL_COLUMNS = Map.of(
            20, List.of("s1_cat_region", "s1_cat_tier", "s1_cat_status", "s1_cat_device", "s1_cat_channel", "s1_cat_segment"),
            50, List.of("s2_cat_zone", "s2_cat_branch", "s2_cat_grade", "s2_cat_type", "s2_cat_tier", "s2_cat_source", "s2_cat_group", "s2_cat_segment", "s2_cat_status", "s2_cat_channel", "s2_cat_role", "s2_cat_dept", "s2_cat_city", "s2_cat_unit", "s2_cat_level", "s2_cat_tier2"),
            100, List.of("s3_c1", "s3_c2", "s3_c3", "s3_c4", "s3_c5", "s3_c6", "s3_c7", "s3_c8", "s3_c9", "s3_c10", "s3_c11", "s3_c12", "s3_c13", "s3_c14", "s3_c15", "s3_c16", "s3_c17", "s3_c18", "s3_c19", "s3_c25", "s3_c21", "s3_c22", "s3_c23", "s3_c24", "s3_c25", "s3_c26", "s3_c27", "s3_c28", "s3_c29", "s3_c30"),
            150, List.of("s4_cat_a1", "s4_cat_a2", "s4_cat_a3", "s4_cat_a4", "s4_cat_a5", "s4_cat_a6", "s4_cat_a7", "s4_cat_a8", "s4_cat_a9", "s4_cat_a10", "s4_cat_a11", "s4_cat_a12", "s4_cat_a13", "s4_cat_a14", "s4_cat_a15", "s4_cat_a16", "s4_cat_a17", "s4_cat_a18", "s4_cat_a19", "s4_cat_a20", "s4_cat_a21", "s4_cat_a22", "s4_cat_a23", "s4_cat_a24", "s4_cat_a25", "s4_cat_a26", "s4_cat_a27", "s4_cat_a28", "s4_cat_a29", "s4_cat_a30", "s4_cat_a31", "s4_cat_a32", "s4_cat_a33", "s4_cat_a34", "s4_cat_a35", "s4_cat_a36", "s4_cat_a37", "s4_cat_a38", "s4_cat_a39", "s4_cat_a45", "s4_cat_a41", "s4_cat_a42", "s4_cat_a43", "s4_cat_a44", "s4_cat_a45", "s4_cat_a46", "s4_cat_a47", "s4_cat_a48", "s4_cat_a49", "s4_cat_a50"),
            200, List.of("s5_tag_1", "s5_tag_2", "s5_tag_3", "s5_tag_4", "s5_tag_5", "s5_tag_6", "s5_tag_7", "s5_tag_8", "s5_tag_9", "s5_tag_10", "s5_tag_11", "s5_tag_12", "s5_tag_13", "s5_tag_14", "s5_tag_15", "s5_tag_16", "s5_tag_17", "s5_tag_18", "s5_tag_19", "s5_tag_20", "s5_tag_21", "s5_tag_22", "s5_tag_23", "s5_tag_24", "s5_tag_25", "s5_tag_26", "s5_tag_27", "s5_tag_28", "s5_tag_29", "s5_tag_30", "s5_tag_31", "s5_tag_32", "s5_tag_33", "s5_tag_34", "s5_tag_35", "s5_tag_36", "s5_tag_37", "s5_tag_38", "s5_tag_39", "s5_tag_40", "s5_tag_41", "s5_tag_42", "s5_tag_43", "s5_tag_44", "s5_tag_45", "s5_tag_46", "s5_tag_47", "s5_tag_48", "s5_tag_49", "s5_tag_50", "s5_tag_51", "s5_tag_52", "s5_tag_53", "s5_tag_54", "s5_tag_55", "s5_tag_56", "s5_tag_57", "s5_tag_58", "s5_tag_59", "s5_tag_65", "s5_tag_61", "s5_tag_62", "s5_tag_63", "s5_tag_64", "s5_tag_65", "s5_tag_66", "s5_tag_67", "s5_tag_68", "s5_tag_69", "s5_tag_70")
    );

    public static final Map<Integer, List<String>> NUMERIC_COLUMNS = Map.of(
            20, List.of("s1_num_age", "s1_num_score", "s1_num_amount", "s1_num_balance", "s1_num_latency", "s1_num_rate", "s1_num_count", "s1_num_index", "s1_num_weight", "s1_num_height", "s1_num_temp", "s1_num_pressure", "s1_num_speed"),
            50, generateNumericNames(50, 34),
            100, generateNumericNames(100, 70),
            150, generateNumericNames(150, 100),
            200, generateNumericNames(200, 130)
    );

    private static List<String> generateNumericNames(int totalFields, int numCount) {
        return java.util.stream.IntStream.range(0, numCount)
                .mapToObj(i -> "schema" + totalFields + "_num_" + i)
                .toList();
    }
}