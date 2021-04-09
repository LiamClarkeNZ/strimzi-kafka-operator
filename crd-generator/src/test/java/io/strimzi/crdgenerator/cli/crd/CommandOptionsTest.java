package io.strimzi.crdgenerator.cli.crd;

import static org.junit.jupiter.api.Assertions.*;

import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.annotations.KubeVersion;
import io.strimzi.crdgenerator.CrdGenerator;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

class CommandOptionsTest {

    @Test
    void testDefaults() throws IOException, ClassNotFoundException {
        CommandOptions noArgsInvocation = new CommandOptions(new String[]{});

        assertEquals(Map.of(), noArgsInvocation.getClasses());
        assertEquals(CrdGenerator.NoneConversionStrategy.class, noArgsInvocation.getConversionStrategy().getClass());
        assertEquals(ApiVersion.V1BETA1, noArgsInvocation.getCrdApiVersion());
        assertEquals(Map.of(), noArgsInvocation.getLabels());
        assertEquals(KubeVersion.parseRange("1.11+"), noArgsInvocation.getTargetKubeVersions());

        assertFalse(noArgsInvocation.isYaml());

        assertNull(noArgsInvocation.getApiVersions());
        assertNull(noArgsInvocation.getDescribeVersions());
        assertNull(noArgsInvocation.getStorageVersion());
    }

    @Test
    void testLabelOptions() throws IOException, ClassNotFoundException {
        CommandOptions multipleLabelsAllowed = new CommandOptions(new String[]{"--label", "a:b", "--label", "c:d"});
        assertEquals(Map.of("a", "b", "c", "d"), multipleLabelsAllowed.getLabels());

        AtomicReference<String> errOutput = new AtomicReference<>();
        CommandOptions invalidLabelsSkipped = new CommandOptions(new String[]{"--label", "e:f", "--label", "invalidBecauseNoColon", "--label", "g:h"}, errOutput::set);

        assertEquals(Map.of("e", "f", "g", "h"), invalidLabelsSkipped.getLabels());
        assertEquals("CrdGenerator: error: Invalid --label invalidBecauseNoColon", errOutput.get());

        CommandOptions trailingColon = new CommandOptions(new String[]{"--label", "whatHappensWhenIEndOnAColon:"}, errOutput::set);

        //TODO check if we should preserve an empty string, or label it an error. Makes sense to label it an error given the above.
        assertEquals(Map.of(), trailingColon.getLabels());
        assertEquals("CrdGenerator: error: Invalid --label whatHappensWhenIEndOnAColon:", errOutput.get());

        CommandOptions evenEmptyLabels = new CommandOptions(new String[]{"--label", "--label"}, errOutput::set);
        assertEquals(Map.of(), evenEmptyLabels.getLabels());
        assertEquals("CrdGenerator: error: Invalid --label --label", errOutput.get());

        List<String> errors = new ArrayList<>();
        CommandOptions oddEmptyLabels = new CommandOptions(new String[]{"--label", "--label", "--label"}, errors::add);
        assertEquals(Map.of(), oddEmptyLabels.getLabels());
        assertEquals(List.of("CrdGenerator: error: Invalid --label --label",
                             "CrdGenerator: error: --label needs an argument in format key:value"), errors);
    }

}