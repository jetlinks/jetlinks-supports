package org.jetlinks.supports.protocol.codec;

import org.jetlinks.core.utils.BytesUtils;

/**
 * 大小端
 */
public enum Endian {
    /**
     * 大端,高位字节在前,低位字节在后.
     */
    BIG {
        @Override
        public long decode(byte[] payload, int offset, int length) {
            return BytesUtils.beToLong(payload, offset, length);
        }

        @Override
        public void encode(long value, byte[] payload, int offset, int length) {
            BytesUtils.numberToBe(payload, value, offset, length);
        }
    } //大端

    ,
    /**
     * 小端,低位字节在前,高位字节在后.
     */
    Little {
        @Override
        public long decode(byte[] payload, int offset, int length) {
            return BytesUtils.leToLong(payload, offset, length);
        }

        @Override
        public void encode(long value, byte[] payload, int offset, int length) {
            BytesUtils.numberToLe(payload, value, offset, length);
        }
    }; //小端


    public abstract long decode(byte[] payload, int offset, int length);

    public abstract void encode(long value, byte[] payload, int offset, int length);
}
