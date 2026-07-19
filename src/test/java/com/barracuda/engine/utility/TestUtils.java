package com.barracuda.engine.utility;

import java.util.concurrent.ThreadLocalRandom;

public final class TestUtils {

    private TestUtils() {}


    public static long randomID() {
        return ThreadLocalRandom.current().nextLong();
    }

}
