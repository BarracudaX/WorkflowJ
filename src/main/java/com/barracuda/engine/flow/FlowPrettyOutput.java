package com.barracuda.engine.flow;

import lombok.Getter;

public class FlowPrettyOutput {

    @Getter
    private final StringBuilder stringBuilder = new StringBuilder();

    private int level = -1;

    public void increaseLevel() {
        level++;
    }

    public void decreaseLevel() {
        level--;
    }

    public String getTab(){
        return "\t".repeat(Math.max(level, 0));
    }

    @Override
    public String toString() {
        return stringBuilder.toString();
    }

}
