package com.hft.risk.dto;

import java.util.List;

public record RiskDecision(boolean approved, List<String> reasons) {
    public static RiskDecision approve() { return new RiskDecision(true, List.of()); }
    public static RiskDecision reject(String reason) { return new RiskDecision(false, List.of(reason)); }
    public static RiskDecision reject(List<String> reasons) { return new RiskDecision(false, reasons); }
}
