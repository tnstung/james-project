package org.apache.james.blob.objectstorage.aws;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class RegionTest {

    @Test
    void shouldNotAcceptNullRegion() {
        assertThatThrownBy(() -> Region.of(null)).isInstanceOf(NullPointerException.class);
    }

}