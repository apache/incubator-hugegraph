package com.baidu.hugegraph.store.client.util;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * @author lynn.bond@hotmail.com
 */
public final class HgUuid {

    private static String encode(UUID uuid) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return Base58.encode(bb.array());
    }

    /**
     * Get a UUID in Base58 FORM
     * @return
     */
    public static String newUUID() {
        return encode(UUID.randomUUID());
    }

}
