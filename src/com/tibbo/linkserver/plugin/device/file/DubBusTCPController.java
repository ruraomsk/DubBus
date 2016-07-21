/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tibbo.linkserver.plugin.device.file;

/**
 *
 * @author Rura
 */
import com.tibbo.aggregate.common.Log;
import com.tibbo.aggregate.common.datatable.*;
import com.tibbo.linkserver.plugin.device.file.item.*;
import java.util.*;
import net.wimpi.modbus.ModbusException;
import net.wimpi.modbus.procimg.*;
import net.wimpi.modbus.util.BitVector;

// Referenced classes of package com.tibbo.linkserver.plugin.device.modbus:
//            ModbusRegister
public class DubBusTCPController {

    private DubBusTCPMaster modbus;

    DubBusTCPController(DubBusTCPMaster modbus) {
        //Log.CORE.info("MultiBusDeviceController in");
        this.modbus = modbus;
    }

    public static DubBusTCPController tcpController(String address, int port,DubBus param) {

        //Log.CORE.info("DubBusDeviceController tcpController in");

        return new DubBusTCPController(new DubBusTCPMaster(address, port,param));
    }

    public void connect()
            throws Exception {
        modbus.connect();
    }

    public void disconnect()
            throws Exception {
        modbus.disconnect();
    }

    public boolean isconnected()
            {
        return modbus.isconnected();
    }

    protected BitVector readCoils(int unitId, int ref, int count)
            throws ModbusException {
        return modbus.readCoils(unitId, ref, count);
    }

    protected BitVector readDiscreteInputs(int unitId, int ref, int count)
            throws ModbusException {
        return modbus.readInputDiscretes(unitId, ref, count);
    }

    protected InputRegister[] readInputRegisters(int unitid, int ref, int count)
            throws ModbusException {
        return modbus.readInputRegisters(unitid, ref, count);
    }

    protected Register[] readHolderRegisters(int unitid, int ref, int count)
            throws ModbusException {
        return modbus.readMultipleRegisters(unitid, ref, count);
    }

    protected void writeCoil(int unitid, int ref, boolean state)
            throws ModbusException {
        modbus.writeCoil(unitid, ref, state);
    }

    protected void writeCoils(int unitid, int ref, BitVector coils)
            throws ModbusException {
        modbus.writeMultipleCoils(unitid, ref, coils);
    }

    protected void writeHolderRegister(int unitid, int ref, Register register)
            throws ModbusException {
        modbus.writeSingleRegister(unitid, ref, register);
    }

    protected void writeHolderRegisters(int unitid, int ref, Register registers[])
            throws ModbusException {
        modbus.writeMultipleRegisters(unitid, ref, registers);
    }

    public DataTable readValue(TableFormat format, ModbusRegister register)
            throws Exception {
        DataTable res = new DataTable(format);
        switch (register.getType()) {
            case 0: // '\0'
            {
                BitVector bv = readCoils(register.getUnitId(), register.getAddress(), register.getSize());
                for (int i = 0; i < bv.size(); i++) {
                    res.addRecord(new Object[]{
                        Boolean.valueOf(bv.getBit(i))
                    });
                }

                break;
            }

            case 1: // '\001'
            {
                BitVector bv = readDiscreteInputs(register.getUnitId(), register.getAddress(), register.getSize());
                for (int i = 0; i < bv.size(); i++) {
                    res.addRecord(new Object[]{
                        Boolean.valueOf(bv.getBit(i))
                    });
                }

                break;
            }

            case 2: // '\002'
            case 3: // '\003'
            {
                BaseItem item = createItemFromRegister(register);
                Object obj;
                for (Iterator i$ = readItem(register.getUnitId(), item).iterator(); i$.hasNext(); res.addRecord(new Object[]{
                    obj
                })) {
                    obj = i$.next();
                }

                break;
            }

            default: {
                throw new IllegalArgumentException((new StringBuilder()).append("Unsupported register type: ").append(register.getType()).toString());
            }
        }
        return res;
    }

    public void writeValue(int unitid, DataTable value, ModbusRegister register)
            throws Exception {
        switch (register.getType()) {
            case 0: // '\0'
            {
                BitVector bv = new BitVector(register.getSize());
                int i = 0;
                for (Iterator i$ = value.iterator(); i$.hasNext();) {
                    DataRecord rec = (DataRecord) i$.next();
                    bv.setBit(i, rec.getBoolean(0).booleanValue());
                    i++;
                }

                if (bv.size() > 1) {
                    writeCoils(unitid, register.getAddress(), bv);
                } else {
                    for (int b = 0; b < bv.size(); b++) {
                        writeCoil(unitid, register.getAddress() + b, bv.getBit(b));
                    }

                }
                break;
            }

            case 3: // '\003'
            {
                BaseItem item = createItemFromRegister(register);
                Object values[] = new Object[value.getRecordCount()];
                int i = 0;
                for (Iterator i$ = value.iterator(); i$.hasNext();) {
                    DataRecord rec = (DataRecord) i$.next();
                    values[i++] = rec.getValue(0);
                }

                writeItem(unitid, item, values);
                break;
            }

            default: {
                throw new IllegalArgumentException((new StringBuilder()).append("Unsupported register type: ").append(register.getType()).toString());
            }
        }
    }

    public void writeItem(int unitid, BaseItem item, Object values[])
            throws ModbusException {
        if (item.getRange() != 3) {
            throw new ModbusException("Register is not a holding input.");
        }
        Register registers[] = new Register[item.getTotalRegisterCount()];
        
        int i = 0;
        Object arr$[] = values;
        int len$ = arr$.length;
        //Log.CORE.info("MultiBusDeviceController writeItem " +Integer.toString(len$) );
        for (int i$ = 0; i$ < len$; i$++) {
            Object value = arr$[i$];
            short shorts[] = item.valueToShorts(value);
            short arr1$[] = shorts;
            int len1$ = arr1$.length;
            for (int ii$ = 0; ii$ < len1$; ii$++) {
                short s = arr1$[ii$];
                //Log.CORE.info("MultiBusDeviceController writeItem ++" + Integer.toString(i)+"="+Integer.toString(s));
                registers[i++] = new SimpleRegister(s);
            }

        }

        if (registers.length > 1) {
            writeHolderRegisters(unitid, item.getOffset(), registers);
        } else {
            for (int r = 0; r < registers.length; r++) {
                Register register = registers[r];
                writeHolderRegister(unitid, item.getOffset() + r, register);
            }

        }
    }

    public List readItem(int unitid, BaseItem item)
            throws ModbusException {
        InputRegister registers[] = ((InputRegister[]) (item.getRange() != 3 ? readInputRegisters(unitid, item.getOffset(), item.getTotalRegisterCount()) : ((InputRegister[]) (readHolderRegisters(unitid, item.getOffset(), item.getTotalRegisterCount())))));

        byte bytes[] = new byte[item.getTotalRegisterCount() * 2];
        int i = 0;

        for (InputRegister register : registers) {
            //Log.CORE.info("MultiBusDeviceController registers " + register.getValue());
            byte arr1$[] = new byte[2];
            arr1$ = register.toBytes();
            int len1$ = arr1$.length;
            for (int ii$ = 0; ii$ < len1$; ii$++) {
                byte b = arr1$[ii$];
                bytes[i++] = b;
            }
        }

        ArrayList result = new ArrayList();
        for (i = 0; i < item.getItemsCount(); i++) {
            result.add(item.bytesToValueRealOffset(bytes, i * item.getItemRegisterCount()));
        }

        return result;
    }

    private BaseItem createItemFromRegister(ModbusRegister register) {
        boolean stringFormat = 18 == register.getFormatForType() || 19 == register.getFormatForType();
        return ((BaseItem) (stringFormat ? new StringItem(register.getType(), register.getAddress(), register.getFormatForType(), register.getSize()) : new NumericItem(register.getType(), register.getAddress(), register.getFormatForType(), register.getSize())));
    }
}
