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
import java.nio.charset.Charset;

// Referenced classes of package com.tibbo.linkserver.plugin.device.modbus.item:
//            BaseItem
public class StringItem extends BaseItem {

    public StringItem(int range, int offset, int dataType, int registerCount) {
        this(range, offset, dataType, registerCount, ASCII);
    }

    public StringItem(int range, int offset, int dataType, int registerCount, Charset charset) {
        super(range, offset, 1);
        this.dataType = dataType;
        this.registerCount = registerCount;
        this.charset = charset;
        validate();
    }

    private void validate() {
        super.validate(registerCount);
        if (range == 0 || range == 1) {
            throw new IllegalStateException("Only binary values can be read from Coil and Input ranges");
        }
        if (dataType != 18 && dataType != 19) {
            throw new IllegalStateException("Invalid data type");
        } else {
            return;
        }
    }

    public int getDataType() {
        return dataType;
    }

    public int getItemRegisterCount() {
        return registerCount;
    }

    public String toString() {
        return (new StringBuilder()).append("StringItem(range=").append(range).append(", offset=").append(offset).append(", dataType=").append(dataType).append(", registerCount=").append(registerCount).append(", charset=").append(charset).append(")").toString();
    }

    public String bytesToValueRealOffset(byte data[], int offset) {
        offset *= 2;
        int length = registerCount * 2;
        if (dataType == 18) {
            return new String(data, offset, length, charset);
        }
        if (dataType == 19) {
            int nullPos = -1;
            int i = offset;
            do {
                if (i >= offset + length) {
                    break;
                }
                if (data[i] == 0) {
                    nullPos = i;
                    break;
                }
                i++;
            } while (true);
            if (nullPos == -1) {
                return new String(data, offset, length, charset);
            } else {
                return new String(data, offset, nullPos, charset);
            }
        } else {
            throw new RuntimeException((new StringBuilder()).append("Unsupported data type: ").append(dataType).toString());
        }
    }

    public short[] valueToShorts(String value) {
        short result[] = new short[registerCount];
        int resultByteLen = registerCount * 2;
        int length;
        if (value != null) {
            byte bytes[] = value.getBytes(charset);
            length = resultByteLen;
            if (length > bytes.length) {
                length = bytes.length;
            }
            for (int i = 0; i < length; i++) {
                setByte(result, i, bytes[i] & 0xff);
            }

        } else {
            length = 0;
        }
        if (dataType == 18) {
            for (int i = length; i < resultByteLen; i++) {
                setByte(result, i, 32);
            }

        } else {
            if (dataType == 19) {
                if (length >= resultByteLen) {
                    result[registerCount - 1] &= 0xff00;
                } else {
                    for (int i = length; i < resultByteLen; i++) {
                        setByte(result, i, 0);
                    }

                }
            } else {
                throw new RuntimeException((new StringBuilder()).append("Unsupported data type: ").append(dataType).toString());
            }
        }
        return result;
    }

    private void setByte(short s[], int byteIndex, int value) {
        if (byteIndex % 2 == 0) {
            s[byteIndex / 2] |= value << 8;
        } else {
            s[byteIndex / 2] |= value;
        }
    }

    @Override
    public short[] valueToShorts(Object x0) {
        return valueToShorts((String) x0);
    }

    public static final Charset ASCII = Charset.forName("ASCII");
    private final int dataType;
    private final int registerCount;
    private final Charset charset;

}
