/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.corporacionfyv.cpemovil;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
/**
 *
 * @author CHRISTYAN
 */
public class Logger {
    private SimpleDateFormat dateFormat;
    private String logFileName;

    public Logger(String logFileName) {
        this.logFileName = logFileName;
        this.dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    public void log(String message) {
        String logMessage = dateFormat.format(new Date()) + " - " + message;
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(logFileName, true))) {
            writer.write(logMessage);
            writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Logger logger = new Logger("mylog.txt");

        logger.log("Inicio del programa");
        logger.log("Algo sucedió");
        logger.log("Ocurrió un error");
    }
}
