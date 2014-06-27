package com.confluex.mule.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class H2Functions {
    public static Logger log = LoggerFactory.getLogger(H2Functions.class);

    public static void myProc(String messageId, String app, String payload) {
        log.debug(String.format("myProc called (%s:%s): %s", app, messageId, payload));
    }
}
