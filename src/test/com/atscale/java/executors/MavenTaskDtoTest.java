package com.atscale.java.executors;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class MavenTaskDtoTest {

    @Test
    public void testSetRunDescriptionSingleArgDoesNotEnhance() {
        MavenTaskDto<Object> dto = new MavenTaskDto<>("task");
        dto.setRunDescription("my description");
        assertEquals("my description", dto.getRunDescription());
    }

    @Test
    public void testSetRunDescriptionWithRunIdEnhances() {
        MavenTaskDto<Object> dto = new MavenTaskDto<>("task");
        dto.setRunId("2026-05-14-ABC123");
        dto.setRunDescription("my description", true);
        String result = dto.getRunDescription();
        assertTrue(result.contains("RunId: 2026-05-14-ABC123"));
        assertTrue(result.contains("my description"));
        assertTrue(result.contains(" ||| "));
    }

    @Test
    public void testSetRunDescriptionWithRunIdFalseDoesNotEnhance() {
        MavenTaskDto<Object> dto = new MavenTaskDto<>("task");
        dto.setRunId("2026-05-14-ABC123");
        dto.setRunDescription("my description", false);
        assertEquals("my description", dto.getRunDescription());
    }

    @Test
    public void testSetRunDescriptionSingleArgDoesNotResetEnhanceFlag() {
        MavenTaskDto<Object> dto = new MavenTaskDto<>("task");
        dto.setRunId("2026-05-14-ABC123");
        dto.setRunDescription("first", true);
        dto.setRunDescription("second");  // single-arg should leave enhanceRunDescription = true
        String result = dto.getRunDescription();
        assertTrue(result.contains("RunId:"));
        assertTrue(result.contains("second"));
    }

    @Test
    public void testGetRunDescriptionDoesNotEnhanceMultipleTimes() {
        MavenTaskDto<Object> dto = new MavenTaskDto<>("task");
        dto.setRunId("2026-05-14-ABC123");
        dto.setRunDescription("my description", true);
        String first = dto.getRunDescription();
        String second = dto.getRunDescription();
        assertEquals(first, second);
        assertEquals(1, second.split("\\|\\|\\|").length - 1); // only one ||| separator
    }

    @Test
    public void testCopyPreservesEnhanceRunDescriptionFlag() {
        MavenTaskDto<Object> original = new MavenTaskDto<>("original");
        original.setRunId("2026-05-14-ABC123");
        original.setRunDescription("my description", true);

        MavenTaskDto<Object> copy = original.copy("copy");
        copy.setRunId("2026-05-14-ABC123"); // copy gets its own runId by default; reset to match

        String result = copy.getRunDescription();
        assertTrue(result.contains("RunId:"), "copy should preserve enhanceRunDescription=true");
        assertTrue(result.contains("my description"));
    }

    @Test
    public void testParseRunDescription() {
        MavenTaskDto<Object> dto = new MavenTaskDto<>("task");
        dto.setRunId("2026-05-14-ABC123");
        dto.setRunDescription("my description", true);

        String enhanced = dto.getRunDescription();
        var parsed = MavenTaskDto.parseRunDescription(enhanced);
        assertEquals("2026-05-14-ABC123", parsed.get("runId"));
        assertEquals("my description", parsed.get("description"));
    }

    @Test
    public void testParseRunDescriptionWithoutRunId() {
        var parsed = MavenTaskDto.parseRunDescription("plain description");
        assertNull(parsed.get("runId"));
        assertEquals("plain description", parsed.get("description"));
    }

    @Test
    public void testEncodeDecodeRoundTrip() {
        String original = "atscale model name with spaces & special chars!";
        String encoded = MavenTaskDto.encode(original);
        assertNotEquals(original, encoded);
        assertEquals(original, MavenTaskDto.decode(encoded));
    }

    @Test
    public void testDecodeNullLiteral() {
        assertNull(MavenTaskDto.decode("null"));
    }

    @Test
    public void testEncodeNullOrEmpty() {
        assertNull(MavenTaskDto.encode(null));
        assertEquals("", MavenTaskDto.encode(""));
    }
}
