package org.jetlinks.supports.official;

import org.jetlinks.core.metadata.*;
import org.jetlinks.core.metadata.types.IntType;
import org.jetlinks.core.metadata.types.StringType;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class JetLinksDeviceMetadataTest {


    @Test
    public void testMergeFunction() {

        JetLinksDeviceMetadata metadata = new JetLinksDeviceMetadata("test", "test");

        metadata.addFunction(SimpleFunctionMetadata.of("test-func",
                                                       "Test",
                                                       Collections.singletonList(SimplePropertyMetadata.of("arg0", "arg0", IntType.GLOBAL)),
                                                       IntType.GLOBAL
        ));

        JetLinksDeviceMetadata metadata2 = new JetLinksDeviceMetadata("test", "test");
        metadata2.addFunction(SimpleFunctionMetadata.of("test-func",
                                                       "Test",
                                                        Collections.singletonList(SimplePropertyMetadata.of("arg0", "arg0", StringType.GLOBAL)),
                                                        IntType.GLOBAL
        ));
        {

            DeviceMetadata merge = metadata.merge(metadata2);

            FunctionMetadata function = merge.getFunctionOrNull("test-func");
            assertNotNull(function);
            assertEquals(function.getInputs().size(), 1);
            assertEquals(function.getInputs().get(0).getValueType(), StringType.GLOBAL);

        }
        {

            DeviceMetadata merge = metadata.merge(metadata2, MergeOption.ignoreExists);
            FunctionMetadata function = merge.getFunctionOrNull("test-func");
            assertNotNull(function);
            assertEquals(function.getInputs().size(), 1);
            assertEquals(function.getInputs().get(0).getValueType(), IntType.GLOBAL);

        }
    }


}