package com.khwu.sparkstreaming._2_basics.sql;

import java.io.Serializable;

public class JavaRow implements Serializable {
    private String user;
    private String text;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }
}