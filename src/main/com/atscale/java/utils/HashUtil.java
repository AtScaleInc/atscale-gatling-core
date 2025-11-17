package com.atscale.java.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.nio.charset.StandardCharsets;
import java.math.BigInteger;

public class HashUtil {

    public static String TO_MD5(String input) {
        try {
            MessageDigest MD5_DIGEST = MessageDigest.getInstance("MD5");
            byte[] inputBytes = input.getBytes();
            byte[] hashBytes = MD5_DIGEST.digest(inputBytes);
            StringBuilder sb = new StringBuilder();
            for (byte b : hashBytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Error generating MD5 hash", e);
        }
    }

    public static String TO_SHA256(String input) {
        try {
            // Get the SHA-256 MessageDigest object
            MessageDigest digest = MessageDigest.getInstance("SHA-256");

            // Generate the hash as a byte array
            // The input string is converted to bytes using UTF-8 charset
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));

            // Convert the byte array into a signum representation BigInteger
            BigInteger number = new BigInteger(1, hash);

            // Convert the message digest into a hexadecimal string
            StringBuilder hexString = new StringBuilder(number.toString(16));

            // Pad with leading zeros to ensure a fixed length of 64 characters (256 bits)
            while (hexString.length() < 64) {
                hexString.insert(0, '0');
            }

            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Error generating SHA-256 hash", e);
        }
    }
}
