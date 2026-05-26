package com.hft.risk.config;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Hot-reloadable risk parameter snapshot. The PUT /api/risk/config endpoint atomically
 * swaps the reference; readers see a consistent snapshot per call.
 */
@Component
@RequiredArgsConstructor
public class RiskParametersHolder {

    private final RiskProperties props;
    private final AtomicReference<RiskParameters> current = new AtomicReference<>();

    public record RiskParameters(
            BigDecimal maxLeverage,
            BigDecimal maxPositionSize,
            BigDecimal maxOrderValue,
            BigDecimal maxDailyLoss,
            int abnormalActivityThreshold,
            int abnormalActivityWindowSeconds,
            BigDecimal priceDeviationPct
    ) {}

    @PostConstruct
    void init() {
        var p = props.getParameters();
        current.set(new RiskParameters(
                p.getMaxLeverage(), p.getMaxPositionSize(), p.getMaxOrderValue(),
                p.getMaxDailyLoss(), p.getAbnormalActivityThreshold(),
                p.getAbnormalActivityWindowSeconds(), p.getPriceDeviationPct()));
    }

    public RiskParameters get() { return current.get(); }

    public void update(RiskParameters params) { current.set(params); }
}
