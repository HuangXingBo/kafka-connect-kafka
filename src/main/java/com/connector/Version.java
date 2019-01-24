package com.connector;

public class Version {
    private static String version = Version.class.getPackage().getImplementationVersion();

    public static String version() {
        return version;
    }
}
