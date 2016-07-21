/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tibbo.linkserver.plugin.device.file.item;


import com.tibbo.aggregate.common.device.DeviceException;

public abstract class BaseItem {

    public static void validateOffset(int offset)
            throws DeviceException {
        if (offset < 0 || offset > 65535) {
            throw new DeviceException((new StringBuilder()).append("Invalid offset: ").append(offset).toString());
        } else {
            return;
        }
    }

    public static void validateEndOffset(int offset)
            throws DeviceException {
        if (offset > 65535) {
            throw new DeviceException((new StringBuilder()).append("Invalid end offset: ").append(offset).toString());
        } else {
            return;
        }
    }

    public BaseItem(int range, int offset, int itemsCount) {
        this.range = range;
        this.offset = offset;
        this.itemsCount = itemsCount;
    }

    protected void validate(int registerCount) {
        try {
            validateOffset(offset);
            validateEndOffset((offset + registerCount) - 1);
        } catch (DeviceException e) {
            throw new IllegalStateException(e);
        }
    }

    public abstract int getDataType();

    public abstract int getItemRegisterCount();

    public int getTotalRegisterCount() {
        return getItemRegisterCount() * itemsCount;
    }

    public int getItemsCount() {
        return itemsCount;
    }

    public int getRange() {
        return range;
    }

    public int getOffset() {
        return offset;
    }

    public int getEndOffset() {
        return (offset + getItemRegisterCount()) - 1;
    }

    public Object bytesToValue(byte data[], int requestOffset) {
        return bytesToValueRealOffset(data, offset - requestOffset);
    }

    
    public abstract Object bytesToValueRealOffset(byte abyte0[], int i);

    public abstract short[] valueToShorts(Object obj);

    protected final int range;
    protected final int offset;
    protected final int itemsCount;
}
