package com.github.stuxuhai.hdata.plugin;


import org.apache.commons.csv.CSVFormat;
/**
 * Created by dog on 4/15/17.
 */
final public class FormatConf {

    public static void confCsvFormat(String format,CSVFormat csvFormat){
        if (format == null) {
            csvFormat = CSVFormat.DEFAULT;
            return;
        }
        switch (format){
            case "excel":
                csvFormat = CSVFormat.EXCEL;
                break;
            case "mysql":
                csvFormat = CSVFormat.MYSQL;
                break;
            case "tdf":
                csvFormat = CSVFormat.TDF;
                break;
            case "rfc4180":
                csvFormat = CSVFormat.RFC4180;
                break;
            default:
                csvFormat = CSVFormat.DEFAULT;
                break;
        }
    }
}
