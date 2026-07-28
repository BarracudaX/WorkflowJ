package com.barracuda.engine.command;

public sealed interface Command {


    record Continue() implements Command {}

    record Reset() implements Command {}

}
