package org.lsm;

import java.nio.ByteBuffer;

public record StorageEntry(String key, String value, boolean isTombstone) {

    public byte[] serialize() {
        byte[] keyBytes = key.getBytes();
        byte[] valBytes = (value!=null) ? value.getBytes() : new byte[0];

        ByteBuffer buffer = ByteBuffer.allocate(4 + keyBytes.length + 4 + valBytes.length + 1);

        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putInt(valBytes.length);
        buffer.put(valBytes);
        buffer.put((byte) (isTombstone ? 1 : 0));

        return buffer.array();
    }

    public static StorageEntry deserialize(ByteBuffer buffer) {
        int keyLen = buffer.getInt();
        byte[] keyBytes = new byte[keyLen];
        buffer.get(keyBytes);

        int valLen = buffer.getInt();
        byte[] valBytes = new byte[valLen];
        buffer.get(valBytes);

        boolean isTombstone = buffer.get() == 1;

        return new StorageEntry(new String(keyBytes), new String(valBytes), isTombstone);
    }
}
