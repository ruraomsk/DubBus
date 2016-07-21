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
import java.math.*;
import org.apache.commons.lang.ArrayUtils;

// Referenced classes of package com.tibbo.linkserver.plugin.device.modbus.item:
//            BaseItem
public class NumericItem extends BaseItem {

    public NumericItem(int range, int offset, int dataType) {
        this(range, offset, dataType, 1);
    }

    public NumericItem(int range, int offset, int dataType, int itemsCount) {
        super(range, offset, itemsCount);
        this.dataType = dataType;
        validate();
    }

    private void validate() {
        super.validate(getItemRegisterCount());
        if (range == 0 || range == 1) {
            throw new IllegalStateException("Only binary values can be read from Coil and Input ranges");
        }
        if (!ArrayUtils.contains(DATA_TYPES, dataType)) {
            throw new IllegalStateException("Invalid data type");
        } else {
            return;
        }
    }

    public int getDataType() {
        return dataType;
    }

    public String toString() {
        return (new StringBuilder()).append("NumericItem(range=").append(range).append(", offset=").append(offset).append(", dataType=").append(dataType).append(")").toString();
    }

    public int getItemRegisterCount() {
        switch (dataType) {
            case 2: // '\002'
            case 3: // '\003'
            case 16: // '\020'
                return 1;

            case 4: // '\004'
            case 5: // '\005'
            case 6: // '\006'
            case 7: // '\007'
            case 8: // '\b'
            case 9: // '\t'
            case 17: // '\021'
                return 2;

            case 10: // '\n'
            case 11: // '\013'
            case 12: // '\f'
            case 13: // '\r'
            case 14: // '\016'
            case 15: // '\017'
                return 4;
        }
        throw new RuntimeException((new StringBuilder()).append("Unsupported data type: ").append(dataType).toString());
    }

    /**
     *
     * @param data
     * @param offset
     * @return
     */
    @Override
    public Object bytesToValueRealOffset(byte data[], int offset) {
        offset *= 2;
        if (dataType == 2) {
            return Integer.valueOf((data[offset] & 0xff) << 8 | data[offset + 1] & 0xff);
        }
        if (dataType == 3) {
            return Short.valueOf((short) ((data[offset] & 0xff) << 8 | data[offset + 1] & 0xff));
        }
        if (dataType == 16) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 2; i++) {
                sb.append(bcdNibbleToInt(data[offset + i], true));
                sb.append(bcdNibbleToInt(data[offset + i], false));
            }

            return Short.valueOf(Short.parseShort(sb.toString()));
        }
        if (dataType == 4) {
            return Long.valueOf((long) (data[offset] & 0xff) << 24 | (long) (data[offset + 1] & 0xff) << 16 | (long) (data[offset + 2] & 0xff) << 8 | (long) (data[offset + 3] & 0xff));
        }
        if (dataType == 5) {
            return Integer.valueOf((data[offset] & 0xff) << 24 | (data[offset + 1] & 0xff) << 16 | (data[offset + 2] & 0xff) << 8 | data[offset + 3] & 0xff);
        }
        if (dataType == 6) {
            return Long.valueOf((long) (data[offset + 2] & 0xff) << 24 | (long) (data[offset + 3] & 0xff) << 16 | (long) (data[offset] & 0xff) << 8 | (long) (data[offset + 1] & 0xff));
        }
        if (dataType == 7) {
            return Integer.valueOf((data[offset + 2] & 0xff) << 24 | (data[offset + 3] & 0xff) << 16 | (data[offset] & 0xff) << 8 | data[offset + 1] & 0xff);
        }
        if (dataType == 8) {
            return Float.valueOf(Float.intBitsToFloat((data[offset] & 0xff) << 24 | (data[offset + 1] & 0xff) << 16 | (data[offset + 2] & 0xff) << 8 | data[offset + 3] & 0xff));
        }
        if (dataType == 9) {
            return Float.valueOf(Float.intBitsToFloat((data[offset + 2] & 0xff) << 24 | (data[offset + 3] & 0xff) << 16 | (data[offset] & 0xff) << 8 | data[offset + 1] & 0xff));
        }
        if (dataType == 17) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 4; i++) {
                sb.append(bcdNibbleToInt(data[offset + i], true));
                sb.append(bcdNibbleToInt(data[offset + i], false));
            }

            return Integer.valueOf(Integer.parseInt(sb.toString()));
        }
        if (dataType == 10) {
            byte b9[] = new byte[9];
            System.arraycopy(data, offset, b9, 1, 8);
            return new BigInteger(b9);
        }
        if (dataType == 11) {
            return Long.valueOf((long) (data[offset] & 0xff) << 56 | (long) (data[offset + 1] & 0xff) << 48 | (long) (data[offset + 2] & 0xff) << 40 | (long) (data[offset + 3] & 0xff) << 32 | (long) (data[offset + 4] & 0xff) << 24 | (long) (data[offset + 5] & 0xff) << 16 | (long) (data[offset + 6] & 0xff) << 8 | (long) (data[offset + 7] & 0xff));
        }
        if (dataType == 12) {
            byte b9[] = new byte[9];
            b9[1] = data[offset + 6];
            b9[2] = data[offset + 7];
            b9[3] = data[offset + 4];
            b9[4] = data[offset + 5];
            b9[5] = data[offset + 2];
            b9[6] = data[offset + 3];
            b9[7] = data[offset];
            b9[8] = data[offset + 1];
            return new BigInteger(b9);
        }
        if (dataType == 13) {
            return Long.valueOf((long) (data[offset + 6] & 0xff) << 56 | (long) (data[offset + 7] & 0xff) << 48 | (long) (data[offset + 4] & 0xff) << 40 | (long) (data[offset + 5] & 0xff) << 32 | (long) (data[offset + 2] & 0xff) << 24 | (long) (data[offset + 3] & 0xff) << 16 | (long) (data[offset] & 0xff) << 8 | (long) (data[offset + 1] & 0xff));
        }
        if (dataType == 14) {
            return Double.valueOf(Double.longBitsToDouble((long) (data[offset] & 0xff) << 56 | (long) (data[offset + 1] & 0xff) << 48 | (long) (data[offset + 2] & 0xff) << 40 | (long) (data[offset + 3] & 0xff) << 32 | (long) (data[offset + 4] & 0xff) << 24 | (long) (data[offset + 5] & 0xff) << 16 | (long) (data[offset + 6] & 0xff) << 8 | (long) (data[offset + 7] & 0xff)));
        }
        if (dataType == 15) {
            return Double.valueOf(Double.longBitsToDouble((long) (data[offset + 6] & 0xff) << 56 | (long) (data[offset + 7] & 0xff) << 48 | (long) (data[offset + 4] & 0xff) << 40 | (long) (data[offset + 5] & 0xff) << 32 | (long) (data[offset + 2] & 0xff) << 24 | (long) (data[offset + 3] & 0xff) << 16 | (long) (data[offset] & 0xff) << 8 | (long) (data[offset + 1] & 0xff)));
        } else {
            throw new RuntimeException((new StringBuilder()).append("Unsupported data type: ").append(dataType).toString());
        }
    }

    private static int bcdNibbleToInt(byte b, boolean high) {
        int n;
        if (high) {
            n = b >> 4 & 0xf;
        } else {
            n = b & 0xf;
        }
        if (n > 9) {
            n = 0;
        }
        return n;
    }

    public short[] valueToShorts(Number value) {
        if (dataType == 2 || dataType == 3) {
            return (new short[]{
                toShort(value)
            });
        }
        if (dataType == 16) {
            short s = toShort(value);
            return (new short[]{
                (short) ((s / 1000) % 10 << 12 | (s / 100) % 10 << 8 | (s / 10) % 10 << 4 | s % 10)
            });
        }
        if (dataType == 4 || dataType == 5) {
            int i = toInt(value);
            return (new short[]{
                (short) (i >> 16), (short) i
            });
        }
        if (dataType == 6 || dataType == 7) {
            int i = toInt(value);
            return (new short[]{
                (short) i, (short) (i >> 16)
            });
        }
        if (dataType == 8) {
            int i = Float.floatToIntBits(value.floatValue());
            return (new short[]{
                (short) (i >> 16), (short) i
            });
        }
        if (dataType == 9) {
            int i = Float.floatToIntBits(value.floatValue());
            return (new short[]{
                (short) i, (short) (i >> 16)
            });
        }
        if (dataType == 17) {
            int i = toInt(value);
            return (new short[]{
                (short) ((i / 0x989680) % 10 << 12 | (i / 0xf4240) % 10 << 8 | (i / 0x186a0) % 10 << 4 | (i / 10000) % 10), (short) ((i / 1000) % 10 << 12 | (i / 100) % 10 << 8 | (i / 10) % 10 << 4 | i % 10)
            });
        }
        if (dataType == 10 || dataType == 11) {
            long l = value.longValue();
            return (new short[]{
                (short) (int) (l >> 48), (short) (int) (l >> 32), (short) (int) (l >> 16), (short) (int) l
            });
        }
        if (dataType == 12 || dataType == 13) {
            long l = value.longValue();
            return (new short[]{
                (short) (int) l, (short) (int) (l >> 16), (short) (int) (l >> 32), (short) (int) (l >> 48)
            });
        }
        if (dataType == 14) {
            long l = Double.doubleToLongBits(value.doubleValue());
            return (new short[]{
                (short) (int) (l >> 48), (short) (int) (l >> 32), (short) (int) (l >> 16), (short) (int) l
            });
        }
        if (dataType == 15) {
            long l = Double.doubleToLongBits(value.doubleValue());
            return (new short[]{
                (short) (int) l, (short) (int) (l >> 16), (short) (int) (l >> 32), (short) (int) (l >> 48)
            });
        } else {
            throw new RuntimeException((new StringBuilder()).append("Unsupported data type: ").append(dataType).toString());
        }
    }

    private short toShort(Number value) {
        return (short) toInt(value);
    }

    private int toInt(Number value) {
        if (value instanceof Double) {
            return (new BigDecimal(value.doubleValue())).setScale(0, RoundingMode.HALF_UP).intValue();
        }
        if (value instanceof Float) {
            return (new BigDecimal(value.floatValue())).setScale(0, RoundingMode.HALF_UP).intValue();
        }
        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).setScale(0, RoundingMode.HALF_UP).intValue();
        } else {
            return value.intValue();
        }
    }

    @Override
    public short[] valueToShorts(Object x0) {
       return valueToShorts((Number) x0);
    }
    
    /**
     *
     * @param x0
     * @param x1
     * @return
     */
    

   
    private static final int DATA_TYPES[] = {
        2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
        12, 13, 14, 15, 16, 17
    };
    private final int dataType;

}
