package com.igot.cb.enrollment.model;

public class KarmaValidationResult {
    private final boolean allowed;
    private final int redeemedKarmaPoints;

    public KarmaValidationResult(boolean allowed, int redeemedKarmaPoints) {
        this.allowed = allowed;
        this.redeemedKarmaPoints = redeemedKarmaPoints;
    }

    public boolean isAllowed() {
        return allowed;
    }

    public int getRedeemedKarmaPoints() {
        return redeemedKarmaPoints;
    }
}
