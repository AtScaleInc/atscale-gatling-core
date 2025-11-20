package com.atscale.java.utils;

import org.junit.jupiter.api.*;

public class HashUtilTest {

    @Test
    public void testToSha256() {
        String input = "test";
        String expectedSha256 = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";
        String actualSha256 = HashUtil.TO_SHA256(input);
        Assertions.assertEquals(expectedSha256, actualSha256);
    }
}
