package com.devcycle.explorestorm.topologies;

public enum OCISDetails {
    CF_THRESHOLD("cf_threshold"), THRESHOLD("threshold");
    private String value;

    public String getValue() {
        return value;
    }

    OCISDetails(String value) {
        this.value = value;
    }
}