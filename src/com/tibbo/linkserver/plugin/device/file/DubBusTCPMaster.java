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
import java.net.InetAddress;
import java.net.UnknownHostException;
import net.wimpi.modbus.ModbusException;
import net.wimpi.modbus.io.ModbusTCPTransaction;
import net.wimpi.modbus.msg.*;
import net.wimpi.modbus.net.TCPMasterConnection;
import net.wimpi.modbus.procimg.InputRegister;
import net.wimpi.modbus.procimg.Register;
import net.wimpi.modbus.util.BitVector;
import com.tibbo.aggregate.common.util.AggreGateThread;
import com.tibbo.aggregate.common.util.ThreadManager;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.wimpi.modbus.ModbusIOException;
import net.wimpi.modbus.ModbusSlaveException;
import net.wimpi.modbus.procimg.SimpleInputRegister;
import net.wimpi.modbus.procimg.SimpleRegister;
// Referenced classes of package com.tibbo.linkserver.plugin.device.modbus.master:
//            ModbusMaster

public class DubBusTCPMaster
{

    private TCPMasterConnection m_Connection;
    private InetAddress m_SlaveAddress;
    private ModbusTCPTransaction m_Transaction;
    private ReadCoilsRequest m_ReadCoilsRequest;
    private ReadInputDiscretesRequest m_ReadInputDiscretesRequest;
    private WriteCoilRequest m_WriteCoilRequest;
    private WriteMultipleCoilsRequest m_WriteMultipleCoilsRequest;
    private ReadInputRegistersRequest m_ReadInputRegistersRequest;
    private ReadMultipleRegistersRequest m_ReadMultipleRegistersRequest;
    private WriteSingleRegisterRequest m_WriteSingleRegisterRequest;
    private WriteMultipleRegistersRequest m_WriteMultipleRegistersRequest;
    private BitVector DataCoils;
    private BitVector DataDI;
    private short[] DataIR;
    private short[] DataHR;
    private int countIR, countHR;
    private boolean m_Reconnecting;
    private int m_Retries;
    public DubBus param;
    private final int MaxLen = 100;
    private ReadDubTCP thRead = null;
    private ThreadManager thrManager = new ThreadManager();
    public boolean flagStop = false;
    int port;

    public ReentrantLock lock = new ReentrantLock();

    public DubBusTCPMaster(String addr, int port, DubBus PARAM)
    {
        this.port = port;
        this.param = PARAM;
        m_Reconnecting = false;
        m_Retries = 3;
        try
        {
            //Log.CORE.info("DubBusTCPMaster in");
            m_SlaveAddress = InetAddress.getByName(addr);
            m_Connection = new TCPMasterConnection(m_SlaveAddress);
            m_Connection.setPort(port);
            m_Connection.setTimeout(500);
            m_ReadCoilsRequest = new ReadCoilsRequest();
            m_ReadInputDiscretesRequest = new ReadInputDiscretesRequest();
            m_WriteCoilRequest = new WriteCoilRequest();
            m_WriteMultipleCoilsRequest = new WriteMultipleCoilsRequest();
            if (param.lenCoils > 0)
            {
                DataCoils = new BitVector(param.lenCoils);
            }
            if (param.lenDI > 0)
            {
                DataDI = new BitVector(param.lenDI);
            }
            if (param.lenIR > 0)
            {
                DataIR = new short[param.lenIR];
            }
            if (param.lenHR > 0)
            {
                DataHR = new short[param.lenHR];
            }
            for (int i = 0; i < param.lenIR; i++)
            {
                DataIR[i] = 0;
            }
            for (int i = 0; i < param.lenHR; i++)
            {
                DataHR[i] = 0;
            }
            //for (Register DataHR1 : DataHR) {
            //    DataHR1.setValue(0);
            //}
            m_ReadInputRegistersRequest = new ReadInputRegistersRequest();
            m_ReadMultipleRegistersRequest = new ReadMultipleRegistersRequest();
            m_WriteSingleRegisterRequest = new WriteSingleRegisterRequest();
            m_WriteMultipleRegistersRequest = new WriteMultipleRegistersRequest();
            //Log.CORE.info("MultiBusTCPMaster out");
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e.getMessage());
        }
    }

    public void connect()
            throws Exception
    {
        //Log.CORE.info("MultiBusTCPMaster connect");
        if (m_Connection != null && !m_Connection.isConnected())
        {

            m_Connection.connect();
            //Log.CORE.info("MultiBusTCPMaster связь установлена");
            m_Transaction = new ModbusTCPTransaction(m_Connection);
            m_Transaction.setReconnecting(m_Reconnecting);
            //Log.CORE.info("MultiBusTCPMaster запускаем поток");
            m_Transaction.setRetries(m_Retries);
            if (!m_Connection.isConnected())
            {
                Log.CORE.info("DubBusTCPMaster is not connect");
                return;
            }
            flagStop = false;
            //readAllCoils();
            //readAllInputDiscretes();
            //readAllInputRegisters();
            //readAllMultipleRegisters();
            if (thRead == null)
            {
                thRead = new ReadDubTCP(this, thrManager);
            }
        }
    }

    public synchronized boolean isconnected()
    {
        return m_Connection.isConnected();
    }

    public synchronized void disconnect()
    {
        if (m_Connection != null && m_Connection.isConnected())
        {
            m_Connection.close();
            m_Transaction = null;
            flagStop = true;
            //try {
            //    Thread.sleep(param.step);
            //} catch (InterruptedException ex) {
            //    Log.CORE.info("MultiBusTCPMaster disconnect don't sleep");
            //}
            if (thRead != null)
            {
                thrManager.interruptThread(thRead);
                try
                {
                    thRead.join();
                }
                catch (InterruptedException ex)
                {
                    Log.CORE.info("MultiBusTCPMaster dont wait thread  " + m_SlaveAddress.toString() + ":" + Integer.toString(port));
                }
                thRead = null;
            }

            thRead = null;
        }
    }

    public void setReconnecting(boolean b)
    {
        m_Reconnecting = b;
        if (m_Transaction != null)
        {
            m_Transaction.setReconnecting(b);
        }
    }

    public void setRetries(int retries)
    {
        m_Retries = retries;
        if (m_Transaction != null)
        {
            m_Transaction.setRetries(retries);
        }
    }

    public boolean isReconnecting()
    {
        return m_Reconnecting;
    }

    public synchronized BitVector readCoils(int unitid, int ref, int count)
    {
        BitVector bv = new BitVector(count);
        //  lock.lock();
        for (int i = 0; i < count; i++)
        {
            bv.setBit(i, DataCoils.getBit(i + ref));
        }
        return bv;
    }

    public synchronized void readAllCoils()
    {
        if (param.lenCoils == 0)
        {
            return;
        }
        if (!isconnected())
        {
            return;
        }
        m_ReadCoilsRequest.setUnitID(1);
        m_ReadCoilsRequest.setReference(0);
        m_ReadCoilsRequest.setBitCount(param.lenCoils);
        m_Transaction.setRequest(m_ReadCoilsRequest);
        try
        {
            m_Transaction.execute();
            DataCoils = ((ReadCoilsResponse) m_Transaction.getResponse()).getCoils();
        }
        catch (ModbusIOException ex)
        {
            Log.CORE.info("DubBusTCPMaster readAllCoils " + ex.toString());
            clearTransaction();
        }
        catch (Exception ex)
        {
            Log.CORE.info("DubBusTCPMaster readAllCoils " + ex.toString());
        }
    }

    public synchronized boolean writeCoil(int unitid, int ref, boolean state)
    {
        //lock.lock();
        m_WriteCoilRequest.setUnitID(unitid);
        m_WriteCoilRequest.setReference(ref);
        m_WriteCoilRequest.setCoil(state);
        m_Transaction.setRequest(m_WriteCoilRequest);
        try
        {
            m_Transaction.execute();
            DataCoils.setBit(ref, state);
        }
        catch (ModbusIOException ex)
        {
            Log.CORE.info("DubBusTCPMaster writeCoil " + ex.toString());
            clearTransaction();
        }
        catch (Exception ex)
        {
            Log.CORE.info("DubBusTCPMaster writeCoil " + ex.toString());
        }
        finally
        {
            //lock.unlock();
        }
        return state;
    }

    public synchronized void writeMultipleCoils(int unitid, int ref, BitVector coils)
    {
        //lock.lock();
        try
        {
            m_WriteMultipleCoilsRequest.setUnitID(unitid);
            m_WriteMultipleCoilsRequest.setReference(ref);
            m_WriteMultipleCoilsRequest.setCoils(coils);
            m_WriteMultipleCoilsRequest.setDataLength(5 + coils.byteSize());
            m_Transaction.setRequest(m_WriteMultipleCoilsRequest);
            m_Transaction.execute();
            for (int i = 0; i < coils.size(); i++)
            {
                DataCoils.setBit(ref + i, coils.getBit(i));
            }
        }
        catch (ModbusIOException ex)
        {
            Log.CORE.info("DubBusTCPMaster writeMultipleCoil" + ex.toString());
            clearTransaction();
        }
        catch (Exception ex)
        {
            Log.CORE.info("DubBusTCPMaster writeMultipleCoil" + ex.toString());
        }
        finally
        {
            //lock.unlock();
        }

    }

    public synchronized BitVector readInputDiscretes(int unitid, int ref, int count)
    {
        BitVector bv = new BitVector(count);
        for (int i = 0; i < count; i++)
        {
            bv.setBit(i, DataDI.getBit(i + ref));
        }
        return bv;
    }

    public synchronized void readAllInputDiscretes()
    {
        if (param.lenDI == 0)
        {
            return;
        }
        if (!isconnected())
        {
            return;
        }
        m_ReadInputDiscretesRequest.setUnitID(1);
        m_ReadInputDiscretesRequest.setReference(0);
        m_ReadInputDiscretesRequest.setBitCount(param.lenDI);
        m_Transaction.setRequest(m_ReadInputDiscretesRequest);
        try
        {
            m_Transaction.execute();
            DataDI = ((ReadInputDiscretesResponse) m_Transaction.getResponse()).getDiscretes();
        }
        catch (ModbusIOException ex)
        {
            Log.CORE.info("DubBusTCPMaster readAllInputDiscrets " + ex.toString());
            clearTransaction();
        }
        catch (Exception ex)
        {
            Log.CORE.info("DubBusTCPMaster readAllInputDiscrets " + ex.toString());
        }
    }

    public synchronized InputRegister[] readInputRegisters(int unitid, int ref, int count)
    {
        InputRegister[] ir = new Register[count];
        for (int i = 0; i < count; i++)
        {
            ir[i] = new SimpleInputRegister(DataIR[ref + i]);
        }
        return ir;
    }

    public synchronized void readAllInputRegisters()
    {
        if (param.lenIR == 0)
        {
            return;
        }
        int ref = 0, count = param.lenIR, len;
        InputRegister[] ir = new InputRegister[MaxLen];
        try
        {
            while (count > 0)
            {
                len = (count > MaxLen) ? MaxLen : count;
                m_ReadInputRegistersRequest.setUnitID(1);
                m_ReadInputRegistersRequest.setReference(ref);
                m_ReadInputRegistersRequest.setWordCount(len);
                m_Transaction.setRequest(m_ReadInputRegistersRequest);
                m_Transaction.execute();
                ir = ((ReadInputRegistersResponse) m_Transaction.getResponse()).getRegisters();
                for (int i = 0; i < len; i++)
                {
                    DataIR[ref + i] = ir[i].toShort();
                }
                count -= MaxLen;
                ref += MaxLen;
            }
        }
        catch (ModbusIOException ex)
        {
            Log.CORE.info("DubBusTCPMaster readAllInputRegisters " + ex.toString());
            clearTransaction();
        }
        catch (Exception ex)
        {
            Log.CORE.info("DubBusTCPMaster readAllInputRegisters " + ex.toString());
        }
    }

    public synchronized void readAllMultipleRegisters()
    {
        if (param.lenHR == 0)
        {
            return;
        }
        if (!isconnected())
        {
            return;
        }
        int ref = 0, count = param.lenHR, len;
        Register[] hr = new Register[MaxLen];
        try
        {
            while (count > 0)
            {
                len = (count > MaxLen) ? MaxLen : count;
                m_ReadMultipleRegistersRequest.setUnitID(1);
                m_ReadMultipleRegistersRequest.setReference(ref);
                m_ReadMultipleRegistersRequest.setWordCount(len);
                m_Transaction.setRequest(m_ReadMultipleRegistersRequest);
                m_Transaction.execute();
                hr = ((ReadMultipleRegistersResponse) m_Transaction.getResponse()).getRegisters();
                for (int i = 0; i < len; i++)
                {
                    DataHR[ref + i] = hr[i].toShort();
                }
                count -= MaxLen;
                ref += MaxLen;
            }
        }
        catch (ModbusIOException ex)
        {
            Log.CORE.info("DubBusTCPMaster readAllMultipleDiscrets " + ex.toString());
            clearTransaction();
        }
        catch (Exception ex)
        {
            Log.CORE.info("DubBusTCPMaster readAllMultipleDiscrets " + ex.toString());
        }
    }

    public synchronized Register[] readMultipleRegisters(int unitid, int ref, int count)
    {
        //Log.CORE.info("MultiBusTCPMaster readMultipleRegisters in " + Integer.toString(ref) + " " + Integer.toString(count));
        Register[] hr = new Register[count];
        for (int i = 0; i < count; i++)
        {
            hr[i] = new SimpleRegister(DataHR[ref + i]);
        }
        return hr;
    }

    public synchronized void writeSingleRegister(int unitid, int ref, Register register)
    {
        //lock.lock();
        try
        {
            m_WriteMultipleRegistersRequest.setUnitID(unitid);
            m_WriteSingleRegisterRequest.setReference(ref);
            m_WriteSingleRegisterRequest.setRegister(register);
            m_Transaction.setRequest(m_WriteSingleRegisterRequest);
            m_Transaction.execute();
            DataHR[ref] = register.toShort();
        }
        catch (ModbusIOException ex)
        {
            Log.CORE.info("DubBusTCPMaster writeSingleRegister " + ex.toString());
            clearTransaction();
        }
        catch (Exception ex)
        {
        }
        finally
        {
            //lock.unlock();
        }

    }

    public synchronized void writeMultipleRegisters(int unitid, int ref, Register registers[])
    {
        //Log.CORE.info("MultiBusTCPMaster writeMultipleRegisters " + Integer.toString(ref) + " " + Integer.toString(registers.length));
        //lock.lock();
        try
        {
            m_WriteMultipleRegistersRequest.setUnitID(unitid);
            m_WriteMultipleRegistersRequest.setReference(ref);
            m_WriteMultipleRegistersRequest.setRegisters(registers);
            m_Transaction.setRequest(m_WriteMultipleRegistersRequest);
            m_Transaction.execute();
            for (int i = 0; i < registers.length; i++)
            {
                DataHR[ref] = registers[i].toShort();
            }
        }
        catch (ModbusIOException ex)
        {
            Log.CORE.info("DubBusTCPMaster writeSingleRegister " + ex.toString());
            clearTransaction();
        }
        catch (Exception ex)
        {
            Log.CORE.info("DubBusTCPMaster writeMultipleRegisters " + ex.toString());
        }
        finally
        {
            //lock.unlock();
        }

    }

    public void setTimeout(int timeout)
    {
        m_Connection.setTimeout(timeout);
    }

    private synchronized void clearTransaction()
    {
        m_Transaction = null;
        m_Connection.close();
        flagStop = true;
        thrManager.interruptThread(thRead);
        thRead = null;
    }

}

class ReadDubTCP extends AggreGateThread
{

    private DubBusTCPMaster fd;
    ThreadManager threadManager = null;

    public ReadDubTCP(DubBusTCPMaster fd, ThreadManager threadManager)
    {
        super(threadManager);
        this.threadManager = threadManager;
        this.fd = fd;
        //Log.CORE.info("Запускаем поток!");
        start();
    }

    @Override
    public void run()
    {
        do
        {
            //Log.CORE.info("В потоке");
            try
            {
                fd.readAllCoils();
                fd.readAllInputDiscretes();
                fd.readAllInputRegisters();
                fd.readAllMultipleRegisters();
                AggreGateThread.sleep(fd.param.step);
            }
            catch (InterruptedException ex)
            {
                return;
            }
        }
        while (!isInterrupted());
    }
}
