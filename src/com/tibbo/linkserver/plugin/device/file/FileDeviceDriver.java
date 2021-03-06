/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tibbo.linkserver.plugin.device.file;

import com.tibbo.aggregate.common.Log;
import com.tibbo.aggregate.common.context.*;
import com.tibbo.aggregate.common.datatable.*;
import com.tibbo.aggregate.common.datatable.validator.LimitsValidator;
import com.tibbo.aggregate.common.datatable.validator.ValidatorHelper;
import com.tibbo.aggregate.common.device.*;
import com.tibbo.aggregate.common.security.ServerPermissionChecker;
import com.tibbo.aggregate.common.util.AggreGateThread;
import com.tibbo.aggregate.common.util.ThreadManager;

import com.tibbo.linkserver.plugin.device.file.item.DataType;
import java.sql.Timestamp;
import java.util.*;
import ruraomsk.list.ru.strongsql.*;

/**
 * Драйвер для Aggregate интерфейса DubBus. Собственно интерфейс постороен на
 * основе ModBus но с учетом того что каждое устройство имеет дублера с таким же
 * набором переменных. Кроме этого весь обмен с утройством производится массивом
 *
 * @author Русинов Юрий <ruraomsk@list.ru>
 */
public class FileDeviceDriver extends AbstractDeviceDriver {

    public FileDeviceDriver() {
        super("dubbus", VFT_CONNECTION_PROPERTIES);
    }

    @Override
    public void setupDeviceContext(DeviceContext deviceContext)
            throws ContextException {
        super.setupDeviceContext(deviceContext);
        deviceContext.setDefaultSynchronizationPeriod(10000L);
        VariableDefinition vd = new VariableDefinition("connectionProperties", VFT_CONNECTION_PROPERTIES, true, true, "connectionProperties", ContextUtils.GROUP_ACCESS);
        vd.setIconId("var_connection");
        vd.setHelpId("ls_drivers_multibus");
        vd.setWritePermissions(ServerPermissionChecker.getManagerPermissions());
        deviceContext.addVariableDefinition(vd);
        vd = new VariableDefinition("registers", VFT_REGISTERS, true, true, "Регистры", ContextUtils.GROUP_ACCESS);
        vd.setWritePermissions(ServerPermissionChecker.getAdminPermissions());
        deviceContext.addVariableDefinition(vd);
        vd = new VariableDefinition("devices", VFT_DEVICES, true, true, "Устройства", ContextUtils.GROUP_ACCESS);
        vd.setWritePermissions(ServerPermissionChecker.getAdminPermissions());
        deviceContext.addVariableDefinition(vd);

        vd = new VariableDefinition("SQLProperties", VFT_SQL, true, true, "Сохранение дампа", ContextUtils.GROUP_ACCESS);
        vd.setWritePermissions(ServerPermissionChecker.getManagerPermissions());
        deviceContext.addVariableDefinition(vd);

        deviceContext.setDeviceType("dubbus");
    }

    @Override
    public List<VariableDefinition> readVariableDefinitions(DeviceEntities entities) throws ContextException, DeviceException, DisconnectionException {
        ensureRegisters();
        return createDeviceVariableDefinitions(registers);
    }

    @Override
    public void accessSettingUpdated(String name) {
        registers = null;
        try {
            if (name.equals("registers")) {
                makeParam();
                ensureRegisters();
            }
        } catch (ContextException ex) {
            Log.CORE.error("Not accessSettingUpdated " + name);;
        }
        super.accessSettingUpdated(name); //To change body of generated methods, choose Tools | Templates.
    }

    public static final String VF_ADDRESS = "address";
    public static final String VF_NAME = "name";
    public static final String VF_REGISTERS_TYPE = "type";
    public static final String VF_REGISTERS_FORMAT = "format";
    public static final String VF_UNIT_ID = "unitId";
    public static final String VF_DESCRIPTION = "description";
    public static final String VF_SIZE = "size";
    public static final String VF_DEVICE = "deviceId";

    private void makeParam() {
        try {
            DeviceContext deviceContext = getDeviceContext();
            DataTable perfs = deviceContext.getVariable("registers", getDeviceContext().getCallerController());
            int lenCoils = 0;
            int lenDI = 0;
            int lenHR = 0;
            int lenIR = 0;
            int type, format, address, size;
            for (DataRecord recperf : perfs) {
                type = recperf.getInt(VF_REGISTERS_TYPE);
                format = recperf.getInt(VF_REGISTERS_FORMAT);
                address = recperf.getInt(VF_ADDRESS);
                size = recperf.getInt(VF_SIZE);

                if (format >= 4 && format <= 9) {
                    size *= 2;
                }
                if (format >= 11 && format <= 15) {
                    size *= 4;
                }
                if (format == 17) {
                    size *= 2;
                }
                int right = address + size;
                switch (type) {
                    case 0:
                        if (right > lenCoils) {
                            lenCoils = right;
                        }
                        break;
                    case 1:
                        if (right > lenDI) {
                            lenDI = right;
                        }
                        break;
                    case 2:
                        if (right > lenIR) {
                            lenIR = right;
                        }
                        break;
                    case 3:
                        if (right > lenHR) {
                            lenHR = right;
                        }
                        break;
                }

            }
            getDeviceContext().setVariableField("connectionProperties", VF_LENCOILS, lenCoils, getDeviceContext().getCallerController());
            getDeviceContext().setVariableField("connectionProperties", VF_LENDI, lenDI, getDeviceContext().getCallerController());
            getDeviceContext().setVariableField("connectionProperties", VF_LENIR, lenIR, getDeviceContext().getCallerController());
            getDeviceContext().setVariableField("connectionProperties", VF_LENHR, lenHR, getDeviceContext().getCallerController());
        } catch (ContextException ex) {
            Log.CORE.error("connectionProperties or registers not found" + ex.getMessage());
        }
    }

    private boolean isWritable(int registerType) {
        switch (registerType) {
            case 1: // '\001'
                return false;

            case 0: // '\0'
                return true;

            case 2: // '\002'
                return false;

            case 3: // '\003'
                return true;
        }
        throw new IllegalArgumentException((new StringBuilder()).append("Unknown register type: ").append(registerType).toString());
    }

    private List createDeviceVariableDefinitions(List registers)
            throws ContextException {
        List res = new LinkedList();
        VariableDefinition vd;
        for (Iterator i$ = registers.iterator(); i$.hasNext(); res.add(vd)) {
            ModbusRegister register = (ModbusRegister) i$.next();
            String name = register.getName();
            int registerType = register.getType();
            int format = register.getFormatForType();
            Character type = (Character) FieldFormat.getClassToTypeMap().get(DataType.getJavaType(format));
            FieldFormat ff = FieldFormat.create(name, type.charValue(), register.getDescription());
            TableFormat rf = new TableFormat(ff);
            rf.setMinRecords(1);
            rf.setMaxRecords(register.getSize());
            rf.setUnresizable(true);
            vd = new VariableDefinition(name, rf, true, isWritable(registerType), register.getDescription(), "remote");
        }
        return res;
    }
    public static final String VF_STEP = "step";
    public static final String VF_CANAL = "canal";
    public static final String VF_LENCOILS = "lenCoils";
    public static final String VF_LENDI = "lenDI";
    public static final String VF_LENIR = "lenIR";
    public static final String VF_LENHR = "lenHR";
    public DubBus PARAM = new DubBus();

    public DubBusTCPController[] controller = new DubBusTCPController[2];
    private int canal = 0;

    @Override
    public void connect()
            throws DeviceException {
        makeParam();
        yesSQL = false;
        DataRecord connProps = null;
        try {
            connProps = getDeviceContext().getVariable("connectionProperties", getDeviceContext().getCallerController()).rec();
        } catch (ContextException ex) {
            Log.CORE.error("connectionProperties not found" + ex.getMessage());
        }
        PARAM.lenCoils = connProps.getInt(VF_LENCOILS);
        PARAM.lenDI = connProps.getInt(VF_LENDI);
        PARAM.lenIR = connProps.getInt(VF_LENIR);
        PARAM.lenHR = connProps.getInt(VF_LENHR);
        PARAM.step = connProps.getInt(VF_STEP);
        for (int i = 0; i < controller.length; i++) {
            controller[i] = null;
        }
        canal = connProps.getInt(VF_CANAL);
        DataTable devs = null;
        try {
            devs = super.getDeviceContext().getVariable("devices", getDeviceContext().getCallerController());
        } catch (ContextException ex) {
            Log.CORE.error("Devices not found");
        }
        Integer errorCount = 0;
        Integer device = 0;
        for (DataRecord recdev : devs) {
            String IPaddres = recdev.getString("IPaddr");
            int port = recdev.getInt("port");
            //Log.CORE.info("Device in " + device.toString()+" "+IPaddres);

            try {

                controller[device] = DubBusTCPController.tcpController(IPaddres, port, PARAM);
                controller[device].connect();
            } catch (Exception ex) {
                Log.CORE.error("Device Error " + device.toString() + " " + ex.getMessage());
                controller[device].disconnect();
                controller[device] = null;
                errorCount++;
            }
            //Log.CORE.info("IPDevice out" + IPaddres);
            device++;
        }
        if (canal == 0 && controller[0] == null) {
            canal = 1;
        }
        if (canal == 1 && controller[1] == null) {
            canal = 0;
        }
        try {
            getDeviceContext().setVariableField("connectionProperties", VF_CANAL, canal, getDeviceContext().getCallerController());
        } catch (ContextException ex) {
            throw new DeviceException("connectionProperties not wrote " + ex.getMessage());
        }
        if (thRead == null) {
            thRead = new DubReconect(this, thrManager);
        }
        super.connect();
        if (errorCount == 2) {
            throw new DeviceException("Not working chanel ");
        }

        try {
            ParamSQL param = new ParamSQL();
            DataRecord rec;
            rec = getDeviceContext().getVariable("SQLProperties", getDeviceContext().getCallerController()).rec();
            param.JDBCDriver = rec.getString("JDBCDriver");
            param.url = rec.getString("url");
            param.user = rec.getString("user");
            param.password = rec.getString("password");
            param.myDB = rec.getString("table");
            boolean initSQL = rec.getBoolean("initSQL");
            long stepSQL = rec.getLong("stepSQL");
            String Tabledescription=rec.getString("description");
            DataTable regData = getDeviceContext().getVariable("registers", getDeviceContext().getCallerController());
            if (initSQL) {
                Log.CORE.info("Создаем базу "+Tabledescription);
                ArrayList<DescrValue> arraydesc = new ArrayList<>();
                int count = 0;
                for (DataRecord reg : regData) {
                    String name = reg.getString("name");
                    String description=reg.getString("description");
                    int type = 1;
                    switch (reg.getInt("format")) {
                        case 2:
                        case 3:
                            if (reg.getInt("type") < 2) {
                                type = 0;
                            } else {
                                type = 1;
                            }
                            break;
                        case 4:
                        case 5:
                            type = 1;
                            break;
                        case 8:
                            type = 2;
                            break;
                        default:
                            Log.CORE.error("Нет типа " + reg.getString("name"));
                    }
                    arraydesc.add(new DescrValue(name,description, type));
                }
                new StrongSql(param, arraydesc,Tabledescription);
                Log.CORE.info("Создали базу "+Tabledescription);
                DataTable cp = getDeviceContext().getVariable("SQLProperties", getDeviceContext().getCallerController());
                cp.rec().setValue("initSQL", false);
                getDeviceContext().setVariable("SQLProperties", getDeviceContext().getCallerController(), cp);

            }
            sqldata = new StrongSql(param, stepSQL);
            sqlseek = new StrongSql(param);
            yesSQL = true;
            Log.CORE.info("Connect "+Tabledescription);
        } catch (ContextException ex) {
            throw new DeviceException("SQL error!");
        }
    }

    private StrongSql sqldata = null;
    private StrongSql sqlseek = null;

    private boolean yesSQL = false;

    private ThreadManager thrManager = new ThreadManager();
    private DubReconect thRead;

    @Override
    public void startSynchronization() throws DeviceException {
        boolean canal0 = false;
        boolean canal1 = false;
        DataRecord connProps = null;
        try {
            connProps = getDeviceContext().getVariable("connectionProperties", getDeviceContext().getCallerController()).rec();
        } catch (ContextException ex) {
            Log.CORE.error("connectionProperties not found" + ex.getMessage());
        }
        canal = connProps.getInt(VF_CANAL);
        Integer errorCount = 0;
        if (controller[0] != null) {
            try {
                if (controller[0].isconnected()) {
                    canal0 = true;
                }
            } catch (Exception ex) {
                canal0 = false;
                errorCount++;
            }
        }
        if (controller[1] != null) {
            try {
                if (controller[1].isconnected()) {
                    canal1 = true;
                }
            } catch (Exception ex) {
                canal1 = false;
                errorCount++;
            }
        }
        int newcanal = canal;
        if ((canal == 0) && !canal0 && canal1) {
            newcanal = 1;
        }
        if ((canal == 1) && !canal1 && canal0) {
            newcanal = 0;
        }

        if (newcanal != canal) {
            canal = newcanal;
            writeNewCanal();
        }
        if (errorCount == 2) {
            super.setConnected(false);
        }
        super.startSynchronization(); //To change body of generated methods, choose Tools | Templates.
    }

    private void writeNewCanal() {
        try {
            getDeviceContext().setVariableField("connectionProperties", VF_CANAL, canal, getDeviceContext().getCallerController());
        } catch (ContextException ex) {
            Log.CORE.error("connectionProperties not wrote " + ex.getMessage());
        }

    }

    @Override
    public void disconnect() throws DeviceException {
        for (DubBusTCPController controller1 : controller) {
            if (controller1 != null) {
                try {
                    controller1.disconnect();
                } catch (Exception ex) {
                    throw new DeviceException(ex);
                }
            }
        }
        thrManager.interruptThread(thRead);
        thRead = null;
        if (yesSQL) {
            sqldata.disconnect();
            sqlseek.disconnect();
        }
        super.disconnect();
    }

    @Override
    public List<FunctionDefinition> readFunctionDefinitions(DeviceEntities entities) throws ContextException, DeviceException, DisconnectionException {
        List res = new LinkedList();
        FieldFormat iff = FieldFormat.create("NewCanal", FieldFormat.INTEGER_FIELD, "Новый номер канала");
        TableFormat inputFormat = new TableFormat(1, 1, iff);
        FieldFormat off = FieldFormat.create("ChangeOutputField", FieldFormat.INTEGER_FIELD, "Результат смены канала");
        TableFormat outputFormat = new TableFormat(1, 1, off);
        FunctionDefinition fd = new FunctionDefinition("ChangeCanal", inputFormat, outputFormat, "Изменить канал ввода", ContextUtils.GROUP_DEFAULT);
        res.add(fd);
        iff = FieldFormat.create("ReConnect", FieldFormat.BOOLEAN_FIELD, "Подтвердите процедуру переподключения");
        inputFormat = new TableFormat(1, 1, iff);
        off = FieldFormat.create("ConnectField", FieldFormat.STRING_FIELD, "Результат переподключения");
        outputFormat = new TableFormat(1, 1, off);
        fd = new FunctionDefinition("ReConnect", inputFormat, outputFormat, "Произвести переподключение", ContextUtils.GROUP_DEFAULT);
        res.add(fd);

        iff = FieldFormat.create("isConnect", FieldFormat.BOOLEAN_FIELD, "Подтвердите Запрос состояния");
        inputFormat = new TableFormat(1, 1, iff);
        outputFormat = new TableFormat(true);
        outputFormat.addField(FieldFormat.create((new StringBuilder()).append("<IPaddr><S><D=").append("IP address устройства").append(">").toString()));
        outputFormat.addField(FieldFormat.create((new StringBuilder()).append("<port><I><A=502><D=").append("Номер порта").append(">").toString()));
        outputFormat.addField(FieldFormat.create((new StringBuilder()).append("<Status><S><D=").append("Статус соединения").append(">").toString()));
        fd = new FunctionDefinition("Status", inputFormat, outputFormat, "Состояние подключения", ContextUtils.GROUP_DEFAULT);
        res.add(fd);

        inputFormat = new TableFormat(1, 1);
        inputFormat.addField(FieldFormat.create("<name><S><D=Имя переменной>"));
        inputFormat.addField(FieldFormat.create("<from><D><D=Начало периода>"));
        inputFormat.addField(FieldFormat.create("<to><D><D=Конец периода>"));
        TableFormat history = new TableFormat(true);
        history.addField(FieldFormat.create("<series><S><D=Имя серии>"));
        history.addField(FieldFormat.create("<x><D><D=Время>"));
        history.addField(FieldFormat.create("<y><F><D=Значение>"));
        fd = new FunctionDefinition("history", inputFormat, history, "История переменной", ContextUtils.GROUP_DEFAULT);
        res.add(fd);

        return res;
    }

    @Override
    public DataTable executeFunction(FunctionDefinition fd, CallerController caller, DataTable parameters) throws ContextException, DeviceException, DisconnectionException {
        if (fd.getName().equals("ChangeCanal")) {
            int newCanal = parameters.rec().getInt("NewCanal");
            if (newCanal == 0 || newCanal == 1) {
                canal = newCanal;
            } else {
                canal = (canal + 1) & 1;
            }
            writeNewCanal();
            return new DataTable(fd.getOutputFormat(), canal);
        }
        if (fd.getName().equals("Status")) {

            boolean flag = parameters.rec().getBoolean("isConnect");
            if (!flag) {
                return new DataTable(fd.getOutputFormat(), "Статус соедениния не запрашивался");
            }
            DataTable devs = null;
            try {
                devs = super.getDeviceContext().getVariable("devices", getDeviceContext().getCallerController());
            } catch (ContextException ex) {
                Log.CORE.info("Devices not found");
            }
            DataTable res = new DataTable(fd.getOutputFormat());

            Integer error = 0;
            Integer device = 0;
            for (DataRecord recdev : devs) {
                DataRecord resrec = new DataRecord(res.getFormat());
                String IPaddres = recdev.getString("IPaddr");
                int port = recdev.getInt("port");
                resrec.setValue("IPaddr", IPaddres);
                resrec.setValue("port", port);
                //Log.CORE.info("Device in " + device.toString()+" "+IPaddres);

                if ((controller[device] != null && controller[device].isconnected())) {
                    resrec.setValue("Status", "Подключено и есть обмен");
                } else if (controller[device] == null) {
                    resrec.setValue("Status", "Отсутствовала связь на момент запуска");
                } else {
                    resrec.setValue("Status", "Во время обмена обнаружены ошибки связи");
                }
                res.addRecord(resrec);
                device++;
            }
            return res;
        }
        if (fd.getName().equals("ReConnect")) {

            boolean flag = parameters.rec().getBoolean("ReConnect");
            if (!flag) {
                return new DataTable(fd.getOutputFormat(), "Переподлючение не производилось");
            }
            DataTable devs = null;
            try {
                devs = super.getDeviceContext().getVariable("devices", getDeviceContext().getCallerController());
            } catch (ContextException ex) {
                Log.CORE.error("Devices not found");
            }
            Integer error = 0;
            Integer device = 0;
            for (DataRecord recdev : devs) {
                String IPaddres = recdev.getString("IPaddr");
                int port = recdev.getInt("port");

                //Log.CORE.info("Device in " + device.toString()+" "+IPaddres);
                try {
                    if ((controller[device] != null && controller[device].isconnected())) {
                        continue;
                    }
                    if (controller[device] == null) {
                        controller[device] = DubBusTCPController.tcpController(IPaddres, port, PARAM);
                        controller[device].connect();
                        continue;
                    }
                    if (!controller[device].isconnected()) {
                        controller[device].disconnect();
                        controller[device].connect();
                    }

                } catch (Exception ex) {
                    Log.CORE.info("Device Error " + device.toString() + " " + ex.getMessage());
                    controller[device] = null;
                    error++;
                }
                device++;
            }

            return new DataTable(fd.getOutputFormat(), "Переподлючение производилось. Ошибок " + error.toString());
        }

        if (fd.getName().equalsIgnoreCase("history")) {
            String svar = parameters.rec().getString("name");
            Timestamp dfrom = new Timestamp(parameters.rec().getDate("from").getTime());
            Timestamp dto = new Timestamp(parameters.rec().getDate("to").getTime());
            DataTable regData = getDeviceContext().getVariable("registers", getDeviceContext().getCallerController());
            DataTable result = new DataTable(fd.getOutputFormat());
            ArrayList<SetValue> asv = sqlseek.seekData(dfrom, dto, svar);
            for (SetValue sv : asv) {
                if (sv.getTime() == 0L) {
                    continue;
                }
                DataRecord rec = result.addRecord();
                rec.setValue("series", svar);
                rec.setValue("x", new Timestamp(sv.getTime()));
                rec.setValue("y", sv.getFloatValue());
            }
            return result;
        }

        return super.executeFunction(fd, caller, parameters); //To change body of generated methods, choose Tools | Templates.
    }

    private void ensureRegisters()
            throws ContextException {
        try {
            if (registers != null) {
                return;
            }
            DataTable regData = getDeviceContext().getVariable("registers", getDeviceContext().getCallerController());
            registers = DataTableConversion.beansFromTable(regData, Class.forName("com.tibbo.linkserver.plugin.device.file.ModbusRegister"), VFT_REGISTERS, true);
            keyMap = new HashMap<>();
            Integer count = 0;
            for (DataRecord reg : regData) {
                String name = reg.getString("name");
                keyMap.put(name, count++);
            }
        } catch (ClassNotFoundException ex) {
            Log.CORE.error("Not Class MultiModbusRegister!");
        } catch (ContextException ex) {
            Log.CORE.error("Ошибка в makeRegisters " + ex.getMessage());
        }

    }
    private HashMap<String, Integer> keyMap;

    @Override
    public DataTable readVariableValue(VariableDefinition vd, CallerController caller)
            throws ContextException, DeviceException, DisconnectionException {
        ModbusRegister reg = getRegister(vd.getName());
        if (!iscanal()) {
            DataTable res = new DataTable(vd.getFormat());
            res.addRecord(0);
            return res;
        }
        try {
            return controller[canal].readValue(vd.getFormat(), reg);
        } catch (Exception ex) {

            throw new DeviceException((new StringBuilder()).append("Failed to get current value of '").append(reg.getName()).append("' (").append(reg.getDescription()).append(") register: ").append(ex.getMessage()).toString(), ex);
        }
    }

    private boolean iscanal() {
        int newcanal = canal == 1 ? 0 : 1;
        boolean b0, b1;
        b0 = isready(0);
        b1 = isready(1);
        if ((canal == 0) && b0) {
            return true;
        }
        if ((canal == 1) && b1) {
            return true;
        }
        if (!b0 && !b0) {
            return false;
        }
        canal = b0 ? 0 : 1;
        return true;
    }

    private boolean isready(int i) {
        if (controller[i] == null) {
            return false;
        }
        if (!controller[i].isconnected()) {
            return false;
        }
        return true;
    }

    @Override
    public void writeVariableValue(VariableDefinition vd, CallerController caller, DataTable value, DataTable deviceValue)
            throws ContextException, DeviceException, DisconnectionException {
        ModbusRegister reg = null;
        try {
            for (int i = 0; i < 2; i++) {
                reg = getRegister(vd.getName());
                if ((controller[i] != null) && (controller[i].isconnected())) {
                    controller[i].writeValue(reg.getUnitId(), value, reg);
                }
            }
        } catch (Exception ex) {
            throw new DeviceException((new StringBuilder()).append("Failed to set value of '").append(reg.getName()).append("' (").append(reg.getDescription()).append(") register: ").append(ex.getMessage()).toString(), ex);
        }
    }

    private ModbusRegister getRegister(String name)
            throws ContextException {
        ensureRegisters();
        for (Iterator idx = registers.iterator(); idx.hasNext();) {
            ModbusRegister register = (ModbusRegister) idx.next();
            if (register.getName().equals(name)) {
                return register;
            }
        }
        return null;
    }

    @Override
    public void finishSynchronization() throws DeviceException, DisconnectionException {
        if (!yesSQL) {
            return;
        }
        try {
            DataTable tregs = getDeviceContext().getVariable("registers", getDeviceContext().getCallerController());

            SetData sd=new SetData(new Timestamp(System.currentTimeMillis()));
            for (DataRecord recregs : tregs) {
                String vname = recregs.getString("name");
                DataTable tvar = getDeviceContext().getVariable(vname, getDeviceContext().getCallerController());
                Object obj = tvar.getRecord(0).getValue(0);
                sd.AddValue(new SetValue(vname, obj));
            }
            if (!sd.datas.isEmpty()) {
                sqldata.addValues(sd);
            }
        } catch (ContextException ex) {
            Log.CORE.error("ContextException " + ex.getMessage());
            throw new DeviceException(ex);
        }
        super.finishSynchronization(); //To change body of generated methods, choose Tools | Templates.
    }

    private static Map registerTypeSelectionValues() {
        Map types = new LinkedHashMap();
        types.put(0, "Дискретный выход (Coil)");
        types.put(1, "Дискретный вход (Discrete Input)");
        types.put(2, "Входной регистр (Input Register)");
        types.put(3, "Выходной регистр (Holding Register)");
        return types;
    }

    private static Map registerFormatSelectionValues() {
        Map reg = new LinkedHashMap();
        reg.put(2, "2-байтный Int Unsigned");
        reg.put(3, "2-байтный Int Signed");
        reg.put(4, "4-байтный Int Unsigned");
        reg.put(5, "4-байтный Int Signed");
        reg.put(6, "4-байтный Int Unsigned Swapped");
        reg.put(7, "4-байтный Int Signed Swapped");
        reg.put(8, "4-байтный Float");
        reg.put(9, "4-байтный Float Swapped");
        reg.put(11, "8-байтный Int Signed");
        reg.put(13, "8-байтный IntSignedSwapped");
        reg.put(14, "8-байтный Float");
        reg.put(15, "8-байтный FloatSwapped");
        reg.put(16, "2-байтный Byte Bcd");
        reg.put(17, "4-байтный Bcd");
        reg.put(18, "Символьный");
        reg.put(19, "Строковый");
        return reg;
    }

    private static final TableFormat VFT_CONNECTION_PROPERTIES;
    private static final TableFormat VFT_REGISTERS;
    //private static final TableFormat VFT_PERFECT;
    private static final TableFormat VFT_DEVICES;
    private static final TableFormat VFT_SQL;
    private List registers;

    static {
        VFT_CONNECTION_PROPERTIES = new TableFormat(1, 1);

        VFT_CONNECTION_PROPERTIES.addField(FieldFormat.create((new StringBuilder()).append("<type><I><D=").append("MultibusVersion").append("><S=<").append("modbusTcp").append("=").append(0).append(">>").toString()));
        VFT_CONNECTION_PROPERTIES.addField(FieldFormat.create((new StringBuilder()).append("<step><I><A=1000><D=").append("Период опроса устройств").append(">").toString()));
        VFT_CONNECTION_PROPERTIES.addField(FieldFormat.create((new StringBuilder()).append("<canal><I><A=0><D=").append("Канал ввода по умолчанию").append(">").toString()));
        VFT_CONNECTION_PROPERTIES.addField(FieldFormat.create((new StringBuilder()).append("<lenCoils><I><D=").append("Размерность Coils").append(">").toString()));
        VFT_CONNECTION_PROPERTIES.addField(FieldFormat.create((new StringBuilder()).append("<lenDI><I><D=").append("Размерность Digital Input").append(">").toString()));
        VFT_CONNECTION_PROPERTIES.addField(FieldFormat.create((new StringBuilder()).append("<lenIR><I><D=").append("Размерность Input Registers").append(">").toString()));
        VFT_CONNECTION_PROPERTIES.addField(FieldFormat.create((new StringBuilder()).append("<lenHR><I><D=").append("Размерность Holding Registers").append(">").toString()));

        VFT_REGISTERS = new TableFormat(true);
        FieldFormat ff = FieldFormat.create((new StringBuilder()).append("<name><S><D=").append("Имя").append(">").toString());
        ff.getValidators().add(ValidatorHelper.NAME_LENGTH_VALIDATOR);
        ff.getValidators().add(ValidatorHelper.NAME_SYNTAX_VALIDATOR);
        VFT_REGISTERS.addField(ff);
        ff = FieldFormat.create((new StringBuilder()).append("<description><S><D=").append("Описание").append(">").toString());
        ff.getValidators().add(new LimitsValidator(1, 200));
        ff.getValidators().add(ValidatorHelper.DESCRIPTION_SYNTAX_VALIDATOR);
        VFT_REGISTERS.addField(ff);
        ff = FieldFormat.create((new StringBuilder()).append("<type><I><D=").append("Тип").append(">").toString());
        ff.setSelectionValues(registerTypeSelectionValues());
        VFT_REGISTERS.addField(ff);
        ff = FieldFormat.create((new StringBuilder()).append("<format><I><D=").append("Формат").append(">").toString());
        ff.setSelectionValues(registerFormatSelectionValues());
        VFT_REGISTERS.addField(ff);
        VFT_REGISTERS.addField(FieldFormat.create((new StringBuilder()).append("<address><I><D=").append("Адрес регистра (десятичный)").append(">").toString()));
        VFT_REGISTERS.addField(FieldFormat.create((new StringBuilder()).append("<size><I><A=1><D=").append("Размер").append("><V=<L=1 255>>").toString()));
        VFT_REGISTERS.addField(FieldFormat.create((new StringBuilder()).append("<unitId><I><A=1><D=").append("unitId").append(">").toString()));
        //VFT_REGISTERS.addField(FieldFormat.create((new StringBuilder()).append("<deviceId><I><A=1><D=").append("Номер устройства").append(">").toString()));
        String ref = "format#enabled";
        String exp = "{type} == 2 || {type} == 3";
        VFT_REGISTERS.addBinding(ref, exp);
        /*
        VFT_PERFECT = new TableFormat(true);
        ff = FieldFormat.create((new StringBuilder()).append("<name><S><D=").append("Имя").append(">").toString());
        ff.getValidators().add(ValidatorHelper.NAME_LENGTH_VALIDATOR);
        ff.getValidators().add(ValidatorHelper.NAME_SYNTAX_VALIDATOR);
        VFT_PERFECT.addField(ff);
        ff = FieldFormat.create((new StringBuilder()).append("<description><S><D=").append("Описание").append(">").toString());
        ff.getValidators().add(new LimitsValidator(Integer.valueOf(1), Integer.valueOf(200)));
        ff.getValidators().add(ValidatorHelper.DESCRIPTION_SYNTAX_VALIDATOR);
        VFT_PERFECT.addField(ff);
        ff = FieldFormat.create((new StringBuilder()).append("<type><I><D=").append("Тип").append(">").toString());
        ff.setSelectionValues(registerTypeSelectionValues());
        VFT_PERFECT.addField(ff);
        ff = FieldFormat.create((new StringBuilder()).append("<format><I><D=").append("Формат").append(">").toString());
        ff.setSelectionValues(registerFormatSelectionValues());
        VFT_PERFECT.addField(ff);
        VFT_PERFECT.addField(FieldFormat.create((new StringBuilder()).append("<address><I><D=").append("Адрес регистра (десятичный)").append(">").toString()));
        VFT_PERFECT.addField(FieldFormat.create((new StringBuilder()).append("<size><I><A=1><D=").append("Размер").append("><V=<L=1 255>>").toString()));
        VFT_PERFECT.addField(FieldFormat.create((new StringBuilder()).append("<unitId><I><A=1><D=").append("unitId").append(">").toString()));
        //VFT_PERFECT.addField(FieldFormat.create((new StringBuilder()).append("<deviceId><I><A=1><D=").append("deviceId").append(">").toString()));
        ref = "format#enabled";
        exp = "{type} == 2 || {type} == 3";
        VFT_PERFECT.addBinding(ref, exp);
         */
        VFT_DEVICES = new TableFormat(true);
        //VFT_DEVICES.addField(FieldFormat.create((new StringBuilder()).append("<prefix><S><D=").append("Префикс к имени ").append(">").toString()));
        VFT_DEVICES.addField(FieldFormat.create((new StringBuilder()).append("<IPaddr><S><D=").append("IP address устройства").append(">").toString()));
        VFT_DEVICES.addField(FieldFormat.create((new StringBuilder()).append("<port><I><A=502><D=").append("Номер порта").append(">").toString()));

        VFT_SQL = new TableFormat(1, 1);
        VFT_SQL.addField(FieldFormat.create("<url><S><A=jdbc:postgresql://localhost:5432/cyclebuff><D=Url базы данных дампов>"));
        VFT_SQL.addField(FieldFormat.create("<JDBCDriver><S><A=org.postgresql.Driver><D=Драйвер базы данных>"));
        VFT_SQL.addField(FieldFormat.create("<table><S><A=DU><D=Таблица хранения переменных>"));
        VFT_SQL.addField(FieldFormat.create("<description><S><A=Переменные с устройства><D=Описание таблицы хранения>"));
        VFT_SQL.addField(FieldFormat.create("<user><S><A=postgres><D=Пользователь>"));
        VFT_SQL.addField(FieldFormat.create("<password><S><A=162747><D=Пароль>"));
        VFT_SQL.addField(FieldFormat.create("<stepSQL><L><A=5000><D=Интервал сохранения переменных в БД >"));
        VFT_SQL.addField(FieldFormat.create("<initSQL><B><A=true><D=Создавать БД при первом запуске>"));
    }

}

class DubReconect extends AggreGateThread {

    private FileDeviceDriver fd;
    ThreadManager threadManager = null;

    public DubReconect(FileDeviceDriver fd, ThreadManager threadManager) {
        super(threadManager);
        this.threadManager = threadManager;
        threadManager.addThread(this);
        this.fd = fd;
        start();
    }

    @Override
    public void run() {
        do {
            //Log.CORE.info("поток!");
            DataTable devs = null;
            try {
                devs = fd.getDeviceContext().getVariable("devices", fd.getDeviceContext().getCallerController());
            } catch (ContextException ex) {
                Log.CORE.error("not devices");
            }
            Integer device = 0;
            for (DataRecord recdev : devs) {
                String IPaddres = recdev.getString("IPaddr");
                int port = recdev.getInt("port");
                //Log.CORE.info("Device in " + device.toString()+" "+IPaddres);
                try {
                    if (!(fd.controller[device] != null && fd.controller[device].isconnected())) {
                        Log.CORE.info("Перезапускаем " + IPaddres.toString() + ":" + Integer.toString(port));

                        if (fd.controller[device] == null) {
                            fd.controller[device] = DubBusTCPController.tcpController(IPaddres, port, fd.PARAM);
                        }

                        fd.controller[device].disconnect();
                        fd.controller[device].connect();
                    }

                } catch (Exception ex) {
                    //Log.CORE.info("Device Error " + device.toString() + " " + ex.getMessage());
                    fd.controller[device] = null;
                }
                device++;
            }
            try {
                AggreGateThread.sleep(10000);
            } catch (InterruptedException ex) {
                //Log.CORE.info("stop driver ");
                return;
            }
        } while (!isInterrupted());
    }

}
