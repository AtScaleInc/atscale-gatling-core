package com.atscale.java.utils;

import org.junit.jupiter.api.*;

public class HashUtilTest {

    public @Test void testToMd5() {
        String input = "test";
        String expectedMd5 = "098f6bcd4621d373cade4e832627b4f6";
        String actualMd5 = HashUtil.TO_MD5(input);
        Assertions.assertEquals(expectedMd5, actualMd5);
    }

    public @Test void testToSha256() throws Exception {
        String input = "test";
        String expectedSha256 = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";
        String actualSha256 = HashUtil.TO_SHA256(input);
        Assertions.assertEquals(expectedSha256, actualSha256);
    }
}
