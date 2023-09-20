////////////////////////////////////////////////////////
//USA ESTE CODIGO PARA GENERAR EL .JAR CON DEPENDECIAS
//mvn clean compile assembly:single
////////////////////////////////////////////////////////
package com.corporacionfyv.cpemovil;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.text.SimpleDateFormat;

/**
 *
 * @author CHRISTYAN
 */
public class cpe {

    private static final List<Registro> registrosAActualizar = new ArrayList<>();

    public static void main(String[] args) {
        com.corporacionfyv.cpemovil.Logger logger = new com.corporacionfyv.cpemovil.Logger("mylog.txt");
        String mysqlServer = "70.40.220.114";
        String mysqlUsername = "geasacpe_factura";
        String mysqlPassword = "#Facturador123";
        String mysqlDatabase = "geasacpe_facturacion";

        // Datos de conexión a SQL Server
        String sqlServer = "CHRISTYAN-PC\\SQLEXPRESS";
        String sqlUsername = "zidigital";
        String sqlPassword = "20512963545";
        String sqlDatabase = "backoffice_fe";

        Connection mysqlConn = null;
        Connection sqlConn = null;
        int registrosProcesados = 0;

        try {

            // Conexión a MySQL
            Class.forName("com.mysql.cj.jdbc.Driver");
            mysqlConn = DriverManager.getConnection(
                    "jdbc:mysql://" + mysqlServer + "/" + mysqlDatabase, mysqlUsername, mysqlPassword);

            // Verificación de la conexión a MySQL
            if (mysqlConn == null) {
                System.out.println("Error en la conexión a MySQL.");
                logger.log("Error en la conexión a MySQL.");
                return;
            }

            // Consulta MySQL para obtener datos
            String mysqlQuery = "SELECT * FROM venta WHERE estado = 0";
            Statement mysqlStatement = mysqlConn.createStatement();
            ResultSet mysqlResult = mysqlStatement.executeQuery(mysqlQuery);

            // Conexión a SQL Server
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            String sqlUrl = "jdbc:sqlserver://" + sqlServer + ";databaseName=" + sqlDatabase + ";encrypt=false;";
            sqlConn = DriverManager.getConnection(sqlUrl, sqlUsername, sqlPassword);

            // Verificación de la conexión a SQL Server
            if (sqlConn == null) {
                System.out.println("Error en la conexión a SQL Server.");
                logger.log("Error en la conexión a SQL Server.");
                return;
            }

            // Procedimiento almacenado en SQL Server
            String procedureName = "Insertar_Venta_Movil";
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-dd-MM HH:mm:ss");

            // Iterar sobre los registros de MySQL y llamar al procedimiento almacenado en SQL Server
            while (mysqlResult.next()) {
                int TpDocIdentidad = mysqlResult.getString("cdtipodoc").startsWith("B") ? 1 : 6;
                String cdtipodoc = mysqlResult.getString("cdtipodoc").startsWith("B") ? "00003" : "00001";
                String AnioMes = new SimpleDateFormat("yyyyMM").format(mysqlResult.getDate("fecdocumento"));
                String nrodocumento = mysqlResult.getString("cdtipodoc") + mysqlResult.getString("nrodocumento");
                String fecdocumento = dateFormat.format(mysqlResult.getTimestamp("fecdocumento"));
                String fecproceso = dateFormat.format(mysqlResult.getTimestamp("fechaproceso"));
                String nropos = mysqlResult.getString("nropos");
                Long cdcliente = mysqlResult.getLong("cdcliente");
                String ruccliente = mysqlResult.getString("ruccliente");
                String rscliente = mysqlResult.getString("rscliente");
                String drcliente = mysqlResult.getString("drcliente");
                double mtototal = mysqlResult.getDouble("mtototal");
                double valorvta = Math.round(mtototal / 1.18 * 100) / 100.0;
                double mtoimpuesto = Math.round((mtototal - valorvta) * 100) / 100.0;
                String nroplaca = mysqlResult.getString("nroplaca");
                String turno = mysqlResult.getString("turno");

                // Llamar al procedimiento almacenado en SQL Server
                CallableStatement sqlProcedure = sqlConn.prepareCall("{call " + procedureName + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}");
                sqlProcedure.setString(1, AnioMes);
                sqlProcedure.setString(2, cdtipodoc);
                sqlProcedure.setString(3, nrodocumento);
                sqlProcedure.setString(4, fecdocumento);
                sqlProcedure.setString(5, fecproceso);
                sqlProcedure.setString(6, nropos);
                sqlProcedure.setLong(7, cdcliente);
                sqlProcedure.setString(8, ruccliente);
                sqlProcedure.setString(9, rscliente);
                sqlProcedure.setString(10, drcliente);
                sqlProcedure.setDouble(11, valorvta);
                sqlProcedure.setDouble(12, mtoimpuesto);
                sqlProcedure.setDouble(13, mtototal);
                sqlProcedure.setString(14, nroplaca);
                sqlProcedure.setString(15, turno);
                sqlProcedure.setInt(16, TpDocIdentidad);

                sqlProcedure.execute();

                // Almacenar los registros a actualizar
                registrosAActualizar.add(new Registro(mysqlResult.getString("cdtipodoc"), mysqlResult.getString("nrodocumento")));

                registrosProcesados++;
            }

            // Actualización del estado en MySQL fuera del while
            for (Registro registro : registrosAActualizar) {
                String updateQuery = "UPDATE venta SET estado = 1 WHERE cdtipodoc = '"
                        + registro.getCdtipodoc() + "' and nrodocumento = '"
                        + registro.getNrodocumento() + "'";
                mysqlStatement.executeUpdate(updateQuery);
            }

            System.out.println("Proceso completado con éxito. Registros procesados: " + registrosProcesados);
            logger.log("Proceso completado con éxito. Registros procesados: " + registrosProcesados);

        } catch (Exception e) {
            e.printStackTrace();
            logger.log("Error: " + e.getMessage());
        } finally {
            try {

                if (mysqlConn != null) {
                    mysqlConn.close();
                }
                if (sqlConn != null) {
                    sqlConn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}

class Registro {

    private String cdtipodoc;
    private String nrodocumento;

    public Registro(String cdtipodoc, String nrodocumento) {
        this.cdtipodoc = cdtipodoc;
        this.nrodocumento = nrodocumento;
    }

    public String getCdtipodoc() {
        return cdtipodoc;
    }

    public String getNrodocumento() {
        return nrodocumento;
    }
}
