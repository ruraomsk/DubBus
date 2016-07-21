/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tibbo.linkserver.plugin.device.file.item;

/**
 *
 * @author Rura
 */
//            BaseItem
public class BinaryItem extends BaseItem {

    public static void validateBit(int bit) {
        if (bit < 0 || bit > 15) {
            throw new IllegalStateException((new StringBuilder()).append("Invalid bit: ").append(bit).toString());
        } else {
            return;
        }
    }

    public static boolean isBinaryRange(int range) {
        return range == 0 || range == 1;
    }

    public BinaryItem(int range, int offset, int count) {
        super(range, offset, count);
        bit = -1;
        if (!isBinaryRange(range)) {
            throw new IllegalStateException("Non-bit requests can only be made from coil status and input status ranges");
        } else {
            validate();
            return;
        }
    }

    public BinaryItem(int range, int offset, int bit, int count) {
        super(range, offset, count);
        this.bit = -1;
        if (isBinaryRange(range)) {
            throw new IllegalStateException("Bit requests can only be made from holding registers and input registers");
        } else {
            this.bit = bit;
            validate();
            return;
        }
    }

    protected void validate() {
        super.validate(1);
        if (!isBinaryRange(range)) {
            validateBit(bit);
        }
    }

    public int getBit() {
        return bit;
    }

    public int getDataType() {
        return 1;
    }

    public int getItemRegisterCount() {
        return 1;
    }

    public String toString() {
        return (new StringBuilder()).append("BinaryItem(range=").append(range).append(", offset=").append(offset).append(", bit=").append(bit).append(")").toString();
    }

    public Boolean bytesToValueRealOffset(byte data[], int offset) {
        if (range == 0 || range == 1) {
            return Boolean.valueOf(((data[offset / 8] & 0xff) >> offset % 8 & 1) == 1);
        } else {
            offset *= 2;
            return Boolean.valueOf(((data[(offset + 1) - bit / 8] & 0xff) >> bit % 8 & 1) == 1);
        }
    }
    @Override
    public short[] valueToShorts(Object x0) {
        return valueToShorts((Boolean) x0);
    }


    private int bit;
}
