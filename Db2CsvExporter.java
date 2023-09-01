package com.garycode;


import java.io.*;
import java.sql.*;
import java.text.*;
import java.util.*;
import java.util.Date;

/**

 */
public class Db2CsvExporter {
    private BufferedWriter fileWriter , fileWriterSQL;

    public void export(String table) throws ClassNotFoundException {


        Properties prop=null; //=readConfig();
        Map<String, String> config =readConfig();
        String whereCause="";

        String jdbcURL = config.get("srcdb");
        String username = config.get("srcusername");
        String password = config.get("srcpassword");




        Class.forName(config.get("driver"));

        List<List<String>> tables =readTableConfig();



        try (Connection connection = DriverManager.getConnection(jdbcURL, username, password)) {

        for (int i=0; i < tables.size()-1; i++ ) {
            List<String> tableInfo= tables.get(i);
            table=tableInfo.get(0);
            whereCause=tableInfo.get(1);

            String csvFileName = getFileName(table.concat("_Export"));
            String fileNameSQL = getFileName(table.concat(".sql"));

            System.out.println("csv file=" + csvFileName);
            String sql = "SELECT * FROM ".concat(table).concat(" where " + whereCause);

            Statement statement = connection.createStatement();

            ResultSet result = statement.executeQuery(sql);

            fileWriter = new BufferedWriter(new FileWriter(csvFileName));
            fileWriterSQL = new BufferedWriter(new FileWriter(fileNameSQL));

            int columnCount = writeHeaderLine(result);

            while (result.next()) {
                String line = "";

                for (int x = 1; x <= columnCount; x++) {
                    Object valueObject = result.getObject(x);
                    String valueString = "";

                    if (valueObject != null) valueString = valueObject.toString();

                    if (valueObject instanceof String) {
                        valueString = "'" + escapeDoubleQuotes(valueString) + "'";
                    } else if (valueObject instanceof java.sql.Date){
                        DateFormat dateFormat = new SimpleDateFormat("dd-MMM-yy");
                        String dateTimeInfo = dateFormat.format(valueObject);
                    }

                    line = line.concat(valueString);

                    if (x != columnCount) {
                        line = line.concat(",");
                    }
                }

                fileWriter.newLine();
                fileWriter.write(line);
                String insertSQL="INSERT INTO " +table + " (" + allColumnName(result) + ") values (" + line + ");";
                fileWriterSQL.write(insertSQL);
                fileWriterSQL.newLine();
            }

            statement.close();
            fileWriter.close();
            fileWriterSQL.close();
        } // end for


        } catch (SQLException e) {
            System.out.println("Datababse error:");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("File IO error:");
            e.printStackTrace();
        }

    }




    public static Properties readxxxConfig() {


        Properties properties = new Properties();
        System.out.println("config file="+ System.getProperty("user.dir")+ "\\src\\com\\garycode\\uat.properties");
        java.net.URL url = ClassLoader.getSystemResource(System.getProperty("user.dir") + "\\src\\uat.properties");


        try  {
            properties.load(url.openStream());
        } catch (FileNotFoundException fie) {
            fie.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }


        //  System.out.println(prop.getProperty("app.name"));
        //   System.out.println(prop.getProperty("app.version"));
        return properties;
    }

    public static List<List<String>> readTableConfig(){
        List<List<String>> records = new ArrayList();
        try  {
            String t=System.getProperty("user.dir");
            BufferedReader br = new BufferedReader(new FileReader(System.getProperty("user.dir") + "\\src\\tables.csv"));
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                records.add(Arrays.asList(values));
            }
/*
            URL testFileUrl = ClassLoader.getSystemResource("tables.csv");
            URI testFileUri = testFileUrl.toURI();
            Map<String, Double> results = new HashMap();
*/


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        return records;
    }



    public static Map<String, String> readConfig(){
        Map <String, String> records = new HashMap();
        try  {
            String t=System.getProperty("user.dir");
            BufferedReader br = new BufferedReader(new FileReader(System.getProperty("user.dir") + "\\src\\uat.properties"));
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split("=");
                String v2=values[1].trim();
               if (values.length>2) {
                   for (int i = 2; i < values.length - 1; i++) {
                       v2 = v2 + values[i].trim();
                   }
               } else
                   v2=values[1].trim();
                records.put(values[0].trim(), v2);

            }
/*
            URL testFileUrl = ClassLoader.getSystemResource("tables.csv");
            URI testFileUri = testFileUrl.toURI();
            Map<String, Double> results = new HashMap();
*/


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        return records;
    }


    private String getFileName(String baseName) {
        DateFormat dateFormat = new SimpleDateFormat("dd-MMM-yy-MM-dd_HH-mm-ss");
        String dateTimeInfo = dateFormat.format(new Date());
        return baseName.concat(String.format("_%s.csv", dateTimeInfo));
    }

    private int writeHeaderLine(ResultSet result) throws SQLException, IOException {
        // write header line containing column names
        ResultSetMetaData metaData = result.getMetaData();
        int numberOfColumns = metaData.getColumnCount();
        String headerLine = "";


         headerLine=allColumnName(result);

        fileWriter.write(headerLine.substring(0, headerLine.length() - 1));

        return numberOfColumns;
    }


    private String allColumnName (ResultSet result ) throws SQLException {
        ResultSetMetaData metaData = result.getMetaData();
        String headerLine="";
        int numberOfColumns= metaData.getColumnCount();

        for (int i = 1; i <= numberOfColumns; i++) {
            String columnName = metaData.getColumnName(i);
            headerLine = headerLine.concat(columnName);
            if (i!=numberOfColumns)  headerLine=headerLine.concat(",");
        }
      return headerLine;
    }

    private String escapeDoubleQuotes(String value) {
        return value.replaceAll("\"", "\"");
    }

    public static void main(String[] args) throws ClassNotFoundException {
        Db2CsvExporter exporter = new Db2CsvExporter();
        exporter.export("gas.stockdata");
//        exporter.export("product");
    }
}