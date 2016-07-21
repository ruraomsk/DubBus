/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tibbo.linkserver.plugin.device.file.item;

import java.math.BigInteger;
import java.lang.*;
public class DataType {

    public DataType() {
    }

    public static int getRegisterCount(int id) {
        switch (id) {
            case 1: // '\001'
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
        return 0;
    }

    public static Class getJavaType(int id) {
        switch (id) {
            case 1: // '\001'java/lang/Boolean
                return java.lang.Boolean.class;

            case 2: // '\002'
                return java.lang.Integer.class;

            case 3: // '\003'
                return java.lang.Integer.class;

            case 4: // '\004'
                return java.lang.Long.class;

            case 5: // '\005'
                return java.lang.Integer.class;

            case 6: // '\006'
                return java.lang.Long.class;

            case 7: // '\007'
                return java.lang.Integer.class;

            case 8: // '\b'
                return java.lang.Float.class;

            case 9: // '\t'
                return java.lang.Float.class;

            case 10: // '\n'
                return java.math.BigInteger.class;

            case 11: // '\013'
                return java.lang.Long.class;

            case 12: // '\f'
                return java.math.BigInteger.class;

            case 13: // '\r'
                return java.lang.Long.class;

            case 14: // '\016'
                return java.lang.Double.class;

            case 15: // '\017'
                return java.lang.Double.class;

            case 16: // '\020'
                return java.lang.Short.class;

            case 17: // '\021'
                return java.lang.Integer.class;

            case 18: // '\022'
            case 19: // '\023'
                return java.lang.String.class;
        }
        return null;
    }

    public static final int BINARY = 1;
    public static final int TWO_BYTE_INT_UNSIGNED = 2;
    public static final int TWO_BYTE_INT_SIGNED = 3;
    public static final int FOUR_BYTE_INT_UNSIGNED = 4;
    public static final int FOUR_BYTE_INT_SIGNED = 5;
    public static final int FOUR_BYTE_INT_UNSIGNED_SWAPPED = 6;
    public static final int FOUR_BYTE_INT_SIGNED_SWAPPED = 7;
    public static final int FOUR_BYTE_FLOAT = 8;
    public static final int FOUR_BYTE_FLOAT_SWAPPED = 9;
    public static final int EIGHT_BYTE_INT_UNSIGNED = 10;
    public static final int EIGHT_BYTE_INT_SIGNED = 11;
    public static final int EIGHT_BYTE_INT_UNSIGNED_SWAPPED = 12;
    public static final int EIGHT_BYTE_INT_SIGNED_SWAPPED = 13;
    public static final int EIGHT_BYTE_FLOAT = 14;
    public static final int EIGHT_BYTE_FLOAT_SWAPPED = 15;
    public static final int TWO_BYTE_BCD = 16;
    public static final int FOUR_BYTE_BCD = 17;
    public static final int CHAR = 18;
    public static final int VARCHAR = 19;
}
