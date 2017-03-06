package com.refactorlabs.cs378.assign7;

/**
 * Created by davidfranke on 9/28/16.
 */

public enum SessionType {
    SUBMITTER("submitter"),
    CLICKER("clicker"),
    SHOWER("shower"),
    VISITOR("visitor"),
    OTHER("other");

    private String text;

    private SessionType(String text) {
        this.text = text;
    }

    public String getText() {
        return text;
    }
}
