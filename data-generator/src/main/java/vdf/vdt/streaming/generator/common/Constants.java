package vdf.vdt.streaming.generator.common;

import vdf.vdt.streaming.generator.model.FieldDefinition;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

// Schema definitions for the CDP event stream.
//
// Legacy 200-field schema (used by rule_gen): 4 typed lists in ratio 1:2:3:4.
//
// Dual-schema data generation (used by data_gen):
//   Schema A - transaction events: 30 fields (3 sc / 6 dc / 9 sn / 12 dn)
//   Schema B - system access logs: 30 fields (3 sc / 6 dc / 9 sn / 12 dn)
//
// Static fields are seeded per customer ID (deterministic).
// Dynamic fields vary each event (global random).
// All monetary amounts are in VND (Vietnamese Dong).
public class Constants {

    public static final int TOTAL_FIELDS = 200;

    public static final int SCHEMA_A_TOTAL_FIELDS = 36;
    public static final int SCHEMA_B_TOTAL_FIELDS = 36;

    // ══════════════════════════════════════════════════════════════════════════
    // 1. STATIC CATEGORICAL  (20 fields, ratio = 1)
    //    Fixed attributes bound to a customer ID – never change across events.
    //    Referenced in rule expressions with the "_current" suffix.
    // ══════════════════════════════════════════════════════════════════════════
    public static final List<FieldDefinition> STATIC_CATEGORICAL_FIELDS =
            Stream.of(

            FieldDefinition.ofEnum("customer_segment",
                    List.of("PREMIUM", "STANDARD", "BASIC", "VIP", "ENTERPRISE")),

            FieldDefinition.ofEnum("home_province",
                    List.of("HANOI", "HCM", "DANANG", "HAIPHONG", "CANTHO",
                            "BINH_DUONG", "DONG_NAI", "QUANG_NINH", "NGHE_AN",
                            "THANH_HOA", "KHANH_HOA", "LAM_DONG", "THAI_NGUYEN",
                            "AN_GIANG", "THUA_THIEN_HUE", "LONG_AN", "BINH_THUAN",
                            "VUNG_TAU", "HA_TINH", "NINH_BINH")),

            FieldDefinition.ofEnum("gender",
                    List.of("MALE", "FEMALE", "OTHER")),

            FieldDefinition.ofEnum("nationality",
                    List.of("VN", "US", "KR", "JP", "CN", "AU", "UK", "FR", "DE", "SG")),

            FieldDefinition.ofEnum("marital_status",
                    List.of("SINGLE", "MARRIED", "DIVORCED", "WIDOWED")),

            FieldDefinition.ofEnum("education_level",
                    List.of("PRIMARY", "SECONDARY", "HIGH_SCHOOL", "BACHELOR", "MASTER", "PHD")),

            FieldDefinition.ofEnum("occupation_category",
                    List.of("EMPLOYEE", "SELF_EMPLOYED", "BUSINESS_OWNER", "STUDENT", "RETIRED", "OTHER")),

            FieldDefinition.ofEnum("income_bracket",
                    List.of("BELOW_5M", "5M_TO_15M", "15M_TO_30M", "30M_TO_50M", "ABOVE_50M")),

            FieldDefinition.ofEnum("residential_area_type",
                    List.of("URBAN", "SUBURBAN", "RURAL")),

            FieldDefinition.ofEnum("bank_tier",
                    List.of("TIER_1", "TIER_2", "TIER_3")),

            FieldDefinition.ofEnum("acquisition_channel",
                    List.of("BRANCH", "ONLINE", "MOBILE_APP", "REFERRAL", "AGENT")),

            FieldDefinition.ofEnum("kyc_status",
                    List.of("VERIFIED", "PENDING", "REJECTED", "EXPIRED")),

            FieldDefinition.ofEnum("risk_rating",
                    List.of("LOW", "MEDIUM", "HIGH", "VERY_HIGH")),

            FieldDefinition.ofEnum("loyalty_tier",
                    List.of("BRONZE", "SILVER", "GOLD", "PLATINUM", "DIAMOND")),

            FieldDefinition.ofEnum("region",
                    List.of("NORTH", "CENTRAL", "SOUTH", "HIGHLAND", "MEKONG_DELTA")),

            FieldDefinition.ofEnum("city_tier",
                    List.of("TIER_1_CITY", "TIER_2_CITY", "TIER_3_CITY")),

            FieldDefinition.ofEnum("employment_sector",
                    List.of("BANKING", "TECH", "HEALTHCARE", "EDUCATION",
                            "RETAIL", "MANUFACTURING", "GOVERNMENT", "OTHER")),

            FieldDefinition.ofEnum("preferred_language",
                    List.of("VI", "EN", "ZH", "KO", "JA")),

            FieldDefinition.ofEnum("customer_type",
                    List.of("INDIVIDUAL", "CORPORATE", "SME")),

            FieldDefinition.ofEnum("credit_bureau_grade",
                    List.of("A", "B", "C", "D", "E"))
    ).map(fd -> fd.withCategory("static_categorical")).toList();

    // ══════════════════════════════════════════════════════════════════════════
    // 2. DYNAMIC CATEGORICAL  (40 fields, ratio = 2)
    //    Fluctuating status attributes that change with every event.
    // ══════════════════════════════════════════════════════════════════════════
    public static final List<FieldDefinition> DYNAMIC_CATEGORICAL_FIELDS =
            Stream.of(

            FieldDefinition.ofEnum("session_status",
                    List.of("ACTIVE", "IDLE", "EXPIRED", "LOCKED")),

            FieldDefinition.ofEnum("login_channel",
                    List.of("MOBILE_APP", "WEB", "ATM", "BRANCH", "CALL_CENTER")),

            FieldDefinition.ofEnum("transaction_type",
                    List.of("TRANSFER", "PAYMENT", "WITHDRAWAL", "DEPOSIT", "TOP_UP", "REFUND")),

            FieldDefinition.ofEnum("device_type",
                    List.of("IOS", "ANDROID", "DESKTOP", "TABLET")),

            FieldDefinition.ofEnum("network_type",
                    List.of("WIFI", "4G", "5G", "3G", "ETHERNET")),

            FieldDefinition.ofEnum("auth_method",
                    List.of("PASSWORD", "OTP", "BIOMETRIC", "PIN", "TOKEN")),

            FieldDefinition.ofEnum("last_transaction_status",
                    List.of("SUCCESS", "FAILED", "PENDING", "REVERSED", "CANCELLED")),

            FieldDefinition.ofEnum("active_product_category",
                    List.of("SAVINGS", "CREDIT_CARD", "PERSONAL_LOAN", "HOME_LOAN", "INSURANCE", "INVESTMENT_FUND")),

            FieldDefinition.ofEnum("notification_preference",
                    List.of("EMAIL", "SMS", "PUSH", "NONE")),

            FieldDefinition.ofEnum("complaint_status",
                    List.of("NONE", "OPEN", "IN_PROGRESS", "RESOLVED", "ESCALATED")),

            FieldDefinition.ofEnum("promotion_eligibility",
                    List.of("ELIGIBLE", "NOT_ELIGIBLE", "PENDING_REVIEW", "OPT_OUT")),

            FieldDefinition.ofEnum("current_location_type",
                    List.of("HOME", "OFFICE", "TRAVEL", "ABROAD", "UNKNOWN")),

            FieldDefinition.ofEnum("card_status",
                    List.of("ACTIVE", "BLOCKED", "EXPIRED", "LOST", "STOLEN")),

            FieldDefinition.ofEnum("loan_repayment_status",
                    List.of("ON_TIME", "OVERDUE_1_30", "OVERDUE_31_90", "OVERDUE_90_PLUS", "NO_LOAN")),

            FieldDefinition.ofEnum("investment_risk_appetite_current",
                    List.of("CONSERVATIVE", "MODERATE", "AGGRESSIVE", "SPECULATIVE")),

            FieldDefinition.ofEnum("contact_outcome",
                    List.of("ANSWERED", "NOT_ANSWERED", "CALLBACK_REQUESTED", "DO_NOT_CONTACT")),

            FieldDefinition.ofEnum("churn_risk_flag",
                    List.of("LOW", "MEDIUM", "HIGH", "CHURNED")),

            FieldDefinition.ofEnum("upsell_flag",
                    List.of("NONE", "TARGETED", "ACCEPTED", "REJECTED")),

            FieldDefinition.ofEnum("fraud_alert_level",
                    List.of("NONE", "LOW", "MEDIUM", "HIGH", "CRITICAL")),

            FieldDefinition.ofEnum("digital_engagement_level",
                    List.of("INACTIVE", "LOW", "MEDIUM", "HIGH", "POWER_USER")),

            FieldDefinition.ofEnum("active_account_type",
                    List.of("CHECKING", "SAVINGS", "CURRENT", "FIXED_DEPOSIT", "MULTI_CURRENCY")),

            FieldDefinition.ofEnum("last_interaction_channel",
                    List.of("MOBILE_APP", "WEB", "ATM", "BRANCH", "CALL_CENTER", "CHATBOT")),

            FieldDefinition.ofEnum("offer_response",
                    List.of("ACCEPTED", "DECLINED", "PENDING", "EXPIRED", "NOT_PRESENTED")),

            FieldDefinition.ofEnum("kyc_refresh_status",
                    List.of("UP_TO_DATE", "REFRESH_NEEDED", "IN_PROGRESS", "OVERDUE")),

            FieldDefinition.ofEnum("email_engagement_status",
                    List.of("OPENED", "CLICKED", "UNSUBSCRIBED", "BOUNCED", "NOT_SENT")),

            FieldDefinition.ofEnum("sms_delivery_status",
                    List.of("DELIVERED", "FAILED", "PENDING", "OPT_OUT")),

            FieldDefinition.ofEnum("current_loan_purpose",
                    List.of("PERSONAL", "BUSINESS", "EDUCATION", "HOME_IMPROVEMENT", "VEHICLE", "TRAVEL", "MEDICAL", "NONE")),

            FieldDefinition.ofEnum("credit_card_usage_status",
                    List.of("ACTIVE", "DORMANT", "OVER_LIMIT", "BLOCKED", "NO_CARD")),

            FieldDefinition.ofEnum("support_ticket_status",
                    List.of("NONE", "OPEN", "IN_PROGRESS", "RESOLVED", "ESCALATED")),

            FieldDefinition.ofEnum("financial_stress_indicator",
                    List.of("NONE", "LOW", "MEDIUM", "HIGH")),

            FieldDefinition.ofEnum("referral_status",
                    List.of("NONE", "REFERRED_OTHERS", "BEING_REFERRED", "CONVERTED")),

            FieldDefinition.ofEnum("last_app_feature_used",
                    List.of("TRANSFER", "QR_PAY", "BILL_PAY", "SAVINGS_GOAL", "INVEST", "INSURANCE", "NONE")),

            FieldDefinition.ofEnum("beneficiary_relationship",
                    List.of("SELF", "FAMILY", "BUSINESS", "THIRD_PARTY", "CHARITY")),

            FieldDefinition.ofEnum("transaction_currency",
                    List.of("VND", "USD", "EUR", "KRW", "JPY", "SGD", "AUD")),

            FieldDefinition.ofEnum("data_consent_status",
                    List.of("FULL_CONSENT", "PARTIAL_CONSENT", "NO_CONSENT", "WITHDRAWN")),

            FieldDefinition.ofEnum("spending_interest_category",
                    List.of("TRAVEL", "DINING", "SHOPPING", "SPORTS", "TECH", "HEALTH", "EDUCATION", "FINANCE")),

            FieldDefinition.ofEnum("installment_payment_active",
                    List.of("YES", "NO")),

            FieldDefinition.ofEnum("overdraft_usage_status",
                    List.of("NONE", "ACTIVE", "OVERDUE")),

            FieldDefinition.ofEnum("wealth_segment_flag",
                    List.of("MASS", "AFFLUENT", "HIGH_NET_WORTH", "ULTRA_HIGH_NET_WORTH")),

            FieldDefinition.ofEnum("recent_trigger_event",
                    List.of("LOGIN", "TRANSACTION", "INQUIRY", "COMPLAINT", "OFFER_VIEW", "PROFILE_UPDATE", "NONE"))
    ).map(fd -> fd.withCategory("dynamic_categorical")).toList();

    // ══════════════════════════════════════════════════════════════════════════
    // 3. STATIC NUMERIC  (60 fields, ratio = 3)
    //    Fixed numeric attributes per customer ID (e.g. age, credit score).
    //    Referenced in rule expressions with the "_current" suffix.
    // ══════════════════════════════════════════════════════════════════════════
    public static final List<FieldDefinition> STATIC_NUMERIC_FIELDS =
            Stream.of(

            // ── Demographic & profile ────────────────────────────────────────
            FieldDefinition.ofIntRange("age",                          18,  100),
            FieldDefinition.ofIntRange("tenure_months",                1,   600),
            FieldDefinition.ofIntRange("employment_years",             0,   50),
            FieldDefinition.ofIntRange("number_of_dependents",         0,   10),
            FieldDefinition.ofIntRange("years_at_current_address",     0,   50),
            FieldDefinition.ofIntRange("preferred_contact_hour",       0,   23),

            // ── Product holdings ─────────────────────────────────────────────
            FieldDefinition.ofIntRange("number_of_products_held",      1,   20),
            FieldDefinition.ofIntRange("total_loan_count",             0,   10),
            FieldDefinition.ofIntRange("total_card_count",             0,   5),
            FieldDefinition.ofIntRange("total_account_count",          1,   10),
            FieldDefinition.ofIntRange("property_ownership_count",     0,   5),
            FieldDefinition.ofIntRange("vehicle_count",                0,   5),

            // ── Credit & risk scores ─────────────────────────────────────────
            FieldDefinition.ofIntRange("base_credit_score",            300, 850),
            FieldDefinition.ofIntRange("kyc_score",                    0,   100),
            FieldDefinition.ofIntRange("aml_risk_score_baseline",      0,   100),
            FieldDefinition.ofIntRange("fraud_risk_score_baseline",    0,   100),
            FieldDefinition.ofIntRange("behavioral_score_baseline",    0,   1000),
            FieldDefinition.ofIntRange("digital_adoption_score",       0,   100),
            FieldDefinition.ofIntRange("engagement_score_baseline",    0,   100),
            FieldDefinition.ofIntRange("financial_literacy_score",     0,   100),

            // ── Propensity scores ─────────────────────────────────────────────
            FieldDefinition.ofIntRange("propensity_churn_score",       0,   100),
            FieldDefinition.ofIntRange("propensity_credit_score",      0,   100),
            FieldDefinition.ofIntRange("propensity_insurance_score",   0,   100),
            FieldDefinition.ofIntRange("propensity_invest_score",      0,   100),
            FieldDefinition.ofIntRange("risk_adjusted_return_score",   0,   100),
            FieldDefinition.ofIntRange("clv_segment_score",            0,   1000),
            FieldDefinition.ofIntRange("nps_score_baseline",           0,   10),
            FieldDefinition.ofIntRange("referral_quality_score",       0,   100),
            FieldDefinition.ofIntRange("social_media_influence_score", 0,   1000),

            // ── Transactional baselines ───────────────────────────────────────
            FieldDefinition.ofIntRange("avg_monthly_transaction_count",    0,   200),
            FieldDefinition.ofIntRange("loyalty_points_balance",           0,   100000),
            FieldDefinition.ofIntRange("total_referrals_made",             0,   50),
            FieldDefinition.ofIntRange("total_complaints_historical",      0,   20),
            FieldDefinition.ofIntRange("base_txn_frequency_per_month",     0,   200),

            // ── Ratio & percentage baselines ─────────────────────────────────
            FieldDefinition.ofFloatRange("max_credit_utilization_pct",      0.0,  100.0),
            FieldDefinition.ofFloatRange("debt_to_income_ratio",            0.0,  10.0),
            FieldDefinition.ofFloatRange("savings_rate_pct",                0.0,  100.0),
            FieldDefinition.ofFloatRange("credit_utilization_ratio_baseline", 0.0, 1.0),
            FieldDefinition.ofFloatRange("mortgage_to_value_ratio",         0.0,  1.0),

            // ── Monetary baselines (VND) ──────────────────────────────────────
            FieldDefinition.ofFloatRange("credit_limit_vnd",                0.0,   500_000_000.0),
            FieldDefinition.ofFloatRange("monthly_income_vnd",              1_000_000.0, 200_000_000.0),
            FieldDefinition.ofFloatRange("base_loan_amount_vnd",            0.0, 5_000_000_000.0),
            FieldDefinition.ofFloatRange("initial_deposit_vnd",             0.0,   100_000_000.0),
            FieldDefinition.ofFloatRange("home_loan_outstanding_vnd",       0.0, 3_000_000_000.0),
            FieldDefinition.ofFloatRange("personal_loan_outstanding_vnd",   0.0,   500_000_000.0),
            FieldDefinition.ofFloatRange("savings_balance_baseline_vnd",    0.0, 1_000_000_000.0),
            FieldDefinition.ofFloatRange("fixed_deposit_total_vnd",         0.0, 2_000_000_000.0),
            FieldDefinition.ofFloatRange("investment_portfolio_vnd",        0.0, 5_000_000_000.0),
            FieldDefinition.ofFloatRange("annual_insurance_premium_vnd",    0.0,    50_000_000.0),
            FieldDefinition.ofFloatRange("base_monthly_expense_vnd",        0.0,   100_000_000.0),
            FieldDefinition.ofFloatRange("max_single_transaction_vnd",      0.0,   500_000_000.0),
            FieldDefinition.ofFloatRange("lifetime_value_estimate_vnd",     0.0, 10_000_000_000.0),
            FieldDefinition.ofFloatRange("avg_monthly_income_6m_vnd",       0.0,   200_000_000.0),
            FieldDefinition.ofFloatRange("avg_monthly_spend_6m_vnd",        0.0,   100_000_000.0),
            FieldDefinition.ofFloatRange("cross_sell_revenue_vnd",          0.0,   100_000_000.0),
            FieldDefinition.ofFloatRange("total_fee_paid_lifetime_vnd",     0.0,   100_000_000.0),
            FieldDefinition.ofFloatRange("total_interest_earned_lifetime_vnd", 0.0, 100_000_000.0),
            FieldDefinition.ofFloatRange("total_interest_paid_lifetime_vnd", 0.0,  500_000_000.0),
            FieldDefinition.ofFloatRange("max_overdraft_approved_vnd",      0.0,   100_000_000.0),

            FieldDefinition.ofIntRange("avg_account_age_months",       0,   600)
    ).map(fd -> fd.withCategory("static_numeric")).toList();

    // ══════════════════════════════════════════════════════════════════════════
    // 4. DYNAMIC NUMERIC  (80 fields, ratio = 4)
    //    Real-time metrics that vary with every event.
    // ══════════════════════════════════════════════════════════════════════════
    public static final List<FieldDefinition> DYNAMIC_NUMERIC_FIELDS =
            Stream.of(

            // ── Score / percentage metrics (small range) ──────────────────────
            FieldDefinition.ofFloatRange("current_credit_utilization_pct",    0.0,   100.0),
            FieldDefinition.ofFloatRange("session_activity_score",            0.0,   100.0),
            FieldDefinition.ofFloatRange("transaction_risk_score",            0.0,   100.0),
            FieldDefinition.ofFloatRange("fraud_probability_score",           0.0,   100.0),
            FieldDefinition.ofFloatRange("real_time_churn_risk_score",        0.0,   100.0),
            FieldDefinition.ofFloatRange("real_time_upsell_score",            0.0,   100.0),
            FieldDefinition.ofFloatRange("real_time_fraud_score",             0.0,   100.0),
            FieldDefinition.ofFloatRange("geolocation_risk_score",            0.0,   100.0),
            FieldDefinition.ofFloatRange("device_trust_score",                0.0,   100.0),
            FieldDefinition.ofFloatRange("ip_reputation_score",               0.0,   100.0),
            FieldDefinition.ofFloatRange("transaction_velocity_score",        0.0,   100.0),
            FieldDefinition.ofFloatRange("behavioral_anomaly_score",          0.0,   100.0),
            FieldDefinition.ofFloatRange("recent_product_interest_score",     0.0,   100.0),
            FieldDefinition.ofFloatRange("campaign_response_rate_pct",        0.0,   100.0),
            FieldDefinition.ofFloatRange("spend_consistency_score",           0.0,   100.0),
            FieldDefinition.ofFloatRange("account_health_score",              0.0,   100.0),
            FieldDefinition.ofFloatRange("digital_transaction_ratio_pct",     0.0,   100.0),
            FieldDefinition.ofFloatRange("cross_sell_acceptance_rate_pct",    0.0,   100.0),
            FieldDefinition.ofFloatRange("savings_goal_completion_pct",       0.0,   100.0),
            FieldDefinition.ofFloatRange("data_consent_score",                0.0,   100.0),
            FieldDefinition.ofFloatRange("merchant_category_diversity_score", 0.0,   100.0),
            FieldDefinition.ofFloatRange("product_usage_breadth_score",       0.0,   100.0),

            // ── Timing metrics ────────────────────────────────────────────────
            FieldDefinition.ofFloatRange("response_latency_ms",               0.0,   5_000.0),
            FieldDefinition.ofFloatRange("session_duration_seconds",          0.0,   3_600.0),
            FieldDefinition.ofFloatRange("idle_time_seconds",                 0.0,   1_800.0),
            FieldDefinition.ofFloatRange("typing_speed_cpm",                  0.0,   500.0),

            // ── Event count metrics ───────────────────────────────────────────
            FieldDefinition.ofIntRange("current_nps_response",                0,     10),
            FieldDefinition.ofIntRange("login_attempts_count",                0,     10),
            FieldDefinition.ofIntRange("failed_transactions_count_today",     0,     20),
            FieldDefinition.ofIntRange("successful_transactions_count_today", 0,     100),
            FieldDefinition.ofIntRange("pages_viewed_session",                0,     200),
            FieldDefinition.ofIntRange("products_viewed_session",             0,     50),
            FieldDefinition.ofIntRange("offers_clicked_today",                0,     20),
            FieldDefinition.ofIntRange("notifications_received_today",        0,     50),
            FieldDefinition.ofIntRange("support_calls_this_month",            0,     20),
            FieldDefinition.ofIntRange("atm_withdrawals_this_week",           0,     20),
            FieldDefinition.ofIntRange("pos_transactions_today",              0,     50),
            FieldDefinition.ofIntRange("online_transfers_today",              0,     30),
            FieldDefinition.ofIntRange("bill_payments_this_month",            0,     50),
            FieldDefinition.ofIntRange("app_session_count_today",             0,     30),

            // ── Real-time monetary amounts (VND, large range) ─────────────────
            FieldDefinition.ofFloatRange("current_balance_vnd",                  0.0, 1_000_000_000.0),
            FieldDefinition.ofFloatRange("last_transaction_amount_vnd",          0.0,   500_000_000.0),
            FieldDefinition.ofFloatRange("daily_spend_total_vnd",                0.0,   100_000_000.0),
            FieldDefinition.ofFloatRange("weekly_spend_total_vnd",               0.0,   500_000_000.0),
            FieldDefinition.ofFloatRange("monthly_spend_total_vnd",              0.0, 2_000_000_000.0),
            FieldDefinition.ofFloatRange("current_credit_card_balance_vnd",      0.0,   500_000_000.0),
            FieldDefinition.ofFloatRange("pending_transaction_amount_vnd",       0.0,   200_000_000.0),
            FieldDefinition.ofFloatRange("atm_withdrawal_amount_today_vnd",      0.0,   100_000_000.0),
            FieldDefinition.ofFloatRange("online_purchase_amount_today_vnd",     0.0,   100_000_000.0),
            FieldDefinition.ofFloatRange("bill_payment_amount_this_month_vnd",   0.0,    50_000_000.0),
            FieldDefinition.ofFloatRange("loan_repayment_amount_this_month_vnd", 0.0,   100_000_000.0),
            FieldDefinition.ofFloatRange("insurance_premium_this_month_vnd",     0.0,    10_000_000.0),
            FieldDefinition.ofFloatRange("savings_deposit_this_month_vnd",       0.0,   200_000_000.0),
            FieldDefinition.ofFloatRange("transfer_amount_today_vnd",            0.0,   500_000_000.0),
            FieldDefinition.ofFloatRange("top_up_amount_today_vnd",              0.0,    10_000_000.0),
            FieldDefinition.ofFloatRange("investment_buy_amount_today_vnd",      0.0,   500_000_000.0),
            FieldDefinition.ofFloatRange("investment_sell_amount_today_vnd",     0.0,   500_000_000.0),
            FieldDefinition.ofFloatRange("forex_exchange_amount_today_vnd",      0.0,   200_000_000.0),
            FieldDefinition.ofFloatRange("overdraft_used_amount_vnd",            0.0,   100_000_000.0),
            FieldDefinition.ofFloatRange("available_balance_vnd",                0.0, 1_000_000_000.0),
            FieldDefinition.ofFloatRange("credit_card_payment_amount_vnd",       0.0,   500_000_000.0),
            FieldDefinition.ofFloatRange("cashback_earned_this_month_vnd",       0.0,    10_000_000.0),
            FieldDefinition.ofFloatRange("late_fee_charged_vnd",                 0.0,     2_000_000.0),
            FieldDefinition.ofFloatRange("interest_charged_this_month_vnd",      0.0,    50_000_000.0),
            FieldDefinition.ofFloatRange("interest_earned_this_month_vnd",       0.0,    20_000_000.0),
            FieldDefinition.ofFloatRange("minimum_payment_due_vnd",              0.0,    50_000_000.0),
            FieldDefinition.ofFloatRange("excess_payment_vnd",                   0.0,   100_000_000.0),
            FieldDefinition.ofFloatRange("merchant_spend_top_category_vnd",      0.0,    50_000_000.0),
            FieldDefinition.ofFloatRange("fixed_deposit_maturity_amount_vnd",    0.0, 2_000_000_000.0),
            FieldDefinition.ofFloatRange("fund_nav_current_vnd",                 0.0,   500_000_000.0),
            FieldDefinition.ofFloatRange("stock_portfolio_value_vnd",            0.0, 5_000_000_000.0),
            FieldDefinition.ofFloatRange("vehicle_loan_outstanding_vnd",         0.0, 1_000_000_000.0),
            FieldDefinition.ofFloatRange("education_loan_outstanding_vnd",       0.0,   500_000_000.0),
            FieldDefinition.ofFloatRange("total_outstanding_debt_vnd",           0.0, 10_000_000_000.0),
            FieldDefinition.ofFloatRange("gold_purchase_amount_vnd",             0.0,   500_000_000.0),
            FieldDefinition.ofFloatRange("reward_points_redeemed_value_vnd",     0.0,     5_000_000.0),
            FieldDefinition.ofFloatRange("referral_bonus_earned_vnd",            0.0,     5_000_000.0),
            FieldDefinition.ofFloatRange("foreign_currency_spend_vnd_equivalent",0.0,   100_000_000.0),
            FieldDefinition.ofFloatRange("insurance_claim_amount_vnd",           0.0, 5_000_000_000.0),
            FieldDefinition.ofFloatRange("property_valuation_vnd",               0.0, 20_000_000_000.0)
    ).map(fd -> fd.withCategory("dynamic_numeric")).toList();

    // ══════════════════════════════════════════════════════════════════════════
    // SCHEMA A — Transaction Events  (30 fields: 3 sc / 6 dc / 9 sn / 12 dn)
    // ══════════════════════════════════════════════════════════════════════════

    public static final List<FieldDefinition> SCHEMA_A_STATIC_CATEGORICAL_FIELDS =
            Stream.of(
            FieldDefinition.ofEnum("customer_segment",
                    List.of("PREMIUM", "STANDARD", "BASIC", "VIP", "ENTERPRISE")),
            FieldDefinition.ofEnum("loyalty_tier",
                    List.of("BRONZE", "SILVER", "GOLD", "PLATINUM", "DIAMOND")),
            FieldDefinition.ofEnum("risk_rating",
                    List.of("LOW", "MEDIUM", "HIGH", "VERY_HIGH"))
    ).map(fd -> fd.withCategory("static_categorical")).toList();

    public static final List<FieldDefinition> SCHEMA_A_DYNAMIC_CATEGORICAL_FIELDS =
            Stream.of(
            FieldDefinition.ofEnum("transaction_type",
                    List.of("TRANSFER", "PAYMENT", "WITHDRAWAL", "DEPOSIT", "TOP_UP", "REFUND")),
            FieldDefinition.ofEnum("last_transaction_status",
                    List.of("SUCCESS", "FAILED", "PENDING", "REVERSED", "CANCELLED")),
            FieldDefinition.ofEnum("card_status",
                    List.of("ACTIVE", "BLOCKED", "EXPIRED", "LOST", "STOLEN")),
            FieldDefinition.ofEnum("active_product_category",
                    List.of("SAVINGS", "CREDIT_CARD", "PERSONAL_LOAN", "HOME_LOAN", "INSURANCE", "INVESTMENT_FUND")),
            FieldDefinition.ofEnum("loan_repayment_status",
                    List.of("ON_TIME", "OVERDUE_1_30", "OVERDUE_31_90", "OVERDUE_90_PLUS", "NO_LOAN")),
            FieldDefinition.ofEnum("transaction_currency",
                    List.of("VND", "USD", "EUR", "KRW", "JPY", "SGD", "AUD"))
    ).map(fd -> fd.withCategory("dynamic_categorical")).toList();

    public static final List<FieldDefinition> SCHEMA_A_STATIC_NUMERIC_FIELDS =
            Stream.of(
            FieldDefinition.ofIntRange("age",                        18,  100),
            FieldDefinition.ofIntRange("base_credit_score",          300, 850),
            FieldDefinition.ofIntRange("tenure_months",              1,   600),
            FieldDefinition.ofIntRange("number_of_products_held",    1,   20),
            FieldDefinition.ofIntRange("total_loan_count",           0,   10),
            FieldDefinition.ofIntRange("total_card_count",           0,   5),
            FieldDefinition.ofFloatRange("credit_limit_vnd",         0.0,   500_000_000.0),
            FieldDefinition.ofFloatRange("monthly_income_vnd",       1_000_000.0, 200_000_000.0),
            FieldDefinition.ofFloatRange("debt_to_income_ratio",     0.0,  10.0)
    ).map(fd -> fd.withCategory("static_numeric")).toList();

    public static final List<FieldDefinition> SCHEMA_A_DYNAMIC_NUMERIC_FIELDS =
            Stream.of(
            FieldDefinition.ofFloatRange("current_balance_vnd",                  0.0, 1_000_000_000.0),
            FieldDefinition.ofFloatRange("last_transaction_amount_vnd",          0.0,   500_000_000.0),
            FieldDefinition.ofFloatRange("daily_spend_total_vnd",                0.0,   100_000_000.0),
            FieldDefinition.ofFloatRange("pending_transaction_amount_vnd",       0.0,   200_000_000.0),
            FieldDefinition.ofFloatRange("current_credit_card_balance_vnd",      0.0,   500_000_000.0),
            FieldDefinition.ofFloatRange("atm_withdrawal_amount_today_vnd",      0.0,   100_000_000.0),
            FieldDefinition.ofFloatRange("transfer_amount_today_vnd",            0.0,   500_000_000.0),
            FieldDefinition.ofFloatRange("loan_repayment_amount_this_month_vnd", 0.0,   100_000_000.0),
            FieldDefinition.ofFloatRange("total_outstanding_debt_vnd",           0.0, 10_000_000_000.0),
            FieldDefinition.ofIntRange("online_transfers_today",                  0,     30),
            FieldDefinition.ofIntRange("failed_transactions_count_today",         0,     20),
            FieldDefinition.ofIntRange("successful_transactions_count_today",     0,     100),
            // LONG and DOUBLE fields — demonstrate distinct type handling (IN/NOT IN for LONG; BETWEEN only for DOUBLE)
            FieldDefinition.ofLongRange("total_transaction_count_lifetime",        0L,    1_000_000L),
            FieldDefinition.ofDoubleRange("average_transaction_amount_vnd",        0.0,   50_000_000.0)
    ).map(fd -> fd.withCategory("dynamic_numeric")).toList();

    // ══════════════════════════════════════════════════════════════════════════
    // SCHEMA B — System Access Logs  (30 fields: 3 sc / 6 dc / 9 sn / 12 dn)
    // ══════════════════════════════════════════════════════════════════════════

    public static final List<FieldDefinition> SCHEMA_B_STATIC_CATEGORICAL_FIELDS =
            Stream.of(
            FieldDefinition.ofEnum("home_province",
                    List.of("HANOI", "HCM", "DANANG", "HAIPHONG", "CANTHO",
                            "BINH_DUONG", "DONG_NAI", "QUANG_NINH", "NGHE_AN",
                            "THANH_HOA", "KHANH_HOA", "LAM_DONG", "THAI_NGUYEN",
                            "AN_GIANG", "THUA_THIEN_HUE", "LONG_AN", "BINH_THUAN",
                            "VUNG_TAU", "HA_TINH", "NINH_BINH")),
            FieldDefinition.ofEnum("preferred_language",
                    List.of("VI", "EN", "ZH", "KO", "JA")),
            FieldDefinition.ofEnum("customer_type",
                    List.of("INDIVIDUAL", "CORPORATE", "SME"))
    ).map(fd -> fd.withCategory("static_categorical")).toList();

    public static final List<FieldDefinition> SCHEMA_B_DYNAMIC_CATEGORICAL_FIELDS =
            Stream.of(
            FieldDefinition.ofEnum("session_status",
                    List.of("ACTIVE", "IDLE", "EXPIRED", "LOCKED")),
            FieldDefinition.ofEnum("login_channel",
                    List.of("MOBILE_APP", "WEB", "ATM", "BRANCH", "CALL_CENTER")),
            FieldDefinition.ofEnum("device_type",
                    List.of("IOS", "ANDROID", "DESKTOP", "TABLET")),
            FieldDefinition.ofEnum("network_type",
                    List.of("WIFI", "4G", "5G", "3G", "ETHERNET")),
            FieldDefinition.ofEnum("auth_method",
                    List.of("PASSWORD", "OTP", "BIOMETRIC", "PIN", "TOKEN")),
            FieldDefinition.ofEnum("current_location_type",
                    List.of("HOME", "OFFICE", "TRAVEL", "ABROAD", "UNKNOWN"))
    ).map(fd -> fd.withCategory("dynamic_categorical")).toList();

    public static final List<FieldDefinition> SCHEMA_B_STATIC_NUMERIC_FIELDS =
            Stream.of(
            FieldDefinition.ofIntRange("digital_adoption_score",          0,   100),
            FieldDefinition.ofIntRange("engagement_score_baseline",        0,   100),
            FieldDefinition.ofIntRange("behavioral_score_baseline",        0,   1000),
            FieldDefinition.ofIntRange("avg_monthly_transaction_count",    0,   200),
            FieldDefinition.ofIntRange("propensity_churn_score",           0,   100),
            FieldDefinition.ofIntRange("nps_score_baseline",               0,   10),
            FieldDefinition.ofIntRange("preferred_contact_hour",           0,   23),
            FieldDefinition.ofIntRange("social_media_influence_score",     0,   1000),
            FieldDefinition.ofIntRange("financial_literacy_score",         0,   100)
    ).map(fd -> fd.withCategory("static_numeric")).toList();

    public static final List<FieldDefinition> SCHEMA_B_DYNAMIC_NUMERIC_FIELDS =
            Stream.of(
            FieldDefinition.ofFloatRange("session_duration_seconds",          0.0,   3_600.0),
            FieldDefinition.ofFloatRange("response_latency_ms",               0.0,   5_000.0),
            FieldDefinition.ofFloatRange("idle_time_seconds",                 0.0,   1_800.0),
            FieldDefinition.ofFloatRange("fraud_probability_score",           0.0,   100.0),
            FieldDefinition.ofFloatRange("behavioral_anomaly_score",          0.0,   100.0),
            FieldDefinition.ofFloatRange("transaction_velocity_score",        0.0,   100.0),
            FieldDefinition.ofIntRange("login_attempts_count",                0,     10),
            FieldDefinition.ofIntRange("pages_viewed_session",                0,     200),
            FieldDefinition.ofIntRange("products_viewed_session",             0,     50),
            FieldDefinition.ofIntRange("app_session_count_today",             0,     30),
            FieldDefinition.ofIntRange("offers_clicked_today",                0,     20),
            FieldDefinition.ofIntRange("notifications_received_today",        0,     50),
            // LONG and DOUBLE fields — demonstrate distinct type handling (IN/NOT IN for LONG; BETWEEN only for DOUBLE)
            FieldDefinition.ofLongRange("total_login_count_lifetime",          0L,    500_000L),
            FieldDefinition.ofDoubleRange("average_session_duration_seconds",  0.0,   3_600.0)
    ).map(fd -> fd.withCategory("dynamic_numeric")).toList();

    // ══════════════════════════════════════════════════════════════════════════
    // SCHEMA A — Nested group: "debt"
    //   4 dynamic numeric fields stored under event.debt.{fieldName}.
    //   Remaining 8 dynamic numeric fields are at the event root level.
    //   Total leaf fields: 3 + 9 + 6 + 8 + 4 = 30.
    // ══════════════════════════════════════════════════════════════════════════
    public static final String SCHEMA_A_NESTED_DYNAMIC_GROUP = "debt";
    public static final Set<String> SCHEMA_A_NESTED_DYNAMIC_FIELD_NAMES = Set.of(
            "loan_repayment_status",
            "transfer_amount_today_vnd",
            "loan_repayment_amount_this_month_vnd",
            "total_outstanding_debt_vnd"
    );

    // ══════════════════════════════════════════════════════════════════════════
    // SCHEMA B — Nested group: "risk_signals"
    //   Mixed dynamic fields (categorical string + numeric) stored under event.risk_signals.{fieldName}.
    // ══════════════════════════════════════════════════════════════════════════
    public static final String SCHEMA_B_NESTED_DYNAMIC_GROUP = "risk_signals";
    public static final Set<String> SCHEMA_B_NESTED_DYNAMIC_FIELD_NAMES = Set.of(
            "session_status",
            "is_suspicious_ip",
            "fraud_probability_score",
            "behavioral_anomaly_score"
    );

    // ══════════════════════════════════════════════════════════════════════════
    // SCHEMA A — TIMESTAMP fields
    // ══════════════════════════════════════════════════════════════════════════
    public static final List<FieldDefinition> SCHEMA_A_STATIC_TIMESTAMP_FIELDS =
            Stream.of(
                FieldDefinition.ofTimestamp("account_opened_date", 1_262_304_000L, 1_704_067_200L)
            ).map(fd -> fd.withCategory("static_categorical")).toList();

    public static final List<FieldDefinition> SCHEMA_A_DYNAMIC_TIMESTAMP_FIELDS =
            Stream.of(
                FieldDefinition.ofTimestamp("last_transaction_time", 1_767_225_600L, 1_787_616_000L)
            ).map(fd -> fd.withCategory("dynamic_categorical")).toList();

    // ══════════════════════════════════════════════════════════════════════════
    // SCHEMA B — TIMESTAMP fields
    // ══════════════════════════════════════════════════════════════════════════
    public static final List<FieldDefinition> SCHEMA_B_STATIC_TIMESTAMP_FIELDS =
            Stream.of(
                FieldDefinition.ofTimestamp("account_created_date", 1_262_304_000L, 1_704_067_200L)
            ).map(fd -> fd.withCategory("static_categorical")).toList();

    public static final List<FieldDefinition> SCHEMA_B_DYNAMIC_TIMESTAMP_FIELDS =
            Stream.of(
                FieldDefinition.ofTimestamp("last_login_time", 1_767_225_600L, 1_787_616_000L)
            ).map(fd -> fd.withCategory("dynamic_categorical")).toList();

    // ══════════════════════════════════════════════════════════════════════════
    // SCHEMA A — BOOLEAN fields (+2 = 34 total leaf fields)
    // ══════════════════════════════════════════════════════════════════════════
    public static final List<FieldDefinition> SCHEMA_A_STATIC_BOOLEAN_FIELDS =
            Stream.of(
                FieldDefinition.ofBoolean("is_vip_member")
            ).map(fd -> fd.withCategory("static_categorical")).toList();

    public static final List<FieldDefinition> SCHEMA_A_DYNAMIC_BOOLEAN_FIELDS =
            Stream.of(
                FieldDefinition.ofBoolean("is_international_transaction")
            ).map(fd -> fd.withCategory("dynamic_categorical")).toList();

    // ══════════════════════════════════════════════════════════════════════════
    // SCHEMA B — BOOLEAN fields (+2 = 34 total leaf fields)
    // ══════════════════════════════════════════════════════════════════════════
    public static final List<FieldDefinition> SCHEMA_B_STATIC_BOOLEAN_FIELDS =
            Stream.of(
                FieldDefinition.ofBoolean("is_2fa_enabled")
            ).map(fd -> fd.withCategory("static_categorical")).toList();

    public static final List<FieldDefinition> SCHEMA_B_DYNAMIC_BOOLEAN_FIELDS =
            Stream.of(
                FieldDefinition.ofBoolean("is_suspicious_ip")
            ).map(fd -> fd.withCategory("dynamic_categorical")).toList();
}