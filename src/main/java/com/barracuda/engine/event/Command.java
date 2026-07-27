package com.barracuda.engine.event;

public sealed interface Command {


    record Continue() implements Command {}

    record Reset() implements Command {}

    record Prepare() implements Command { }

}
