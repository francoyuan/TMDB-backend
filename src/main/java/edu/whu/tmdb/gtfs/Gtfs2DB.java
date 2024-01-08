package edu.whu.tmdb.gtfs;/*
 * className:qtfs2DB
 * Package:edu.whu.tmdb.qtfs
 * Description:
 * @Author: xyl
 * @Create:2024/1/6 - 16:45
 * @Version:v1
 */

import org.apache.commons.cli.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Scanner;



public class Gtfs2DB {

    //use QTFS to build SQLite database
    //QTFSPath: the path of QTFS
    //DBPath: the path of SQLite database
    public static void buildDB(String gtfsPath, String dbPath) {
        //use gtfs-to-sqlite package to build SQLite database
        try {
            Path databasePath = Path.of(dbPath);
            if (Files.exists(databasePath, new LinkOption[0])) {
                System.out.println(("A file at " + databasePath.toString() + " already exists."));
                System.out.print("Would you like to replace it? (yes/no) ");
                Scanner reader = new Scanner(System.in);
                String response = reader.nextLine();
                if (response.equals("yes")) {
                    System.out.println("Deleting file...");
                    Files.delete(databasePath);
                    System.out.println("File deleted.");
                } else {
                    if (!response.equals("no")) {
                        throw new Exception("Unrecognised response.");
                    }

                    System.exit(0);
                }

                reader.close();
            }

            Connection connection = DriverManager.getConnection("jdbc:sqlite:" + databasePath.toAbsolutePath());
            connection.setAutoCommit(false);
            File gtfsFile=new File(gtfsPath);
            new Loader(gtfsFile, connection);
            connection.close();
            System.out.println("Database created.");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
