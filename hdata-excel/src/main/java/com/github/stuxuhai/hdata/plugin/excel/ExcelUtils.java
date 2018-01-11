package com.github.stuxuhai.hdata.plugin.excel;

import org.apache.poi.ss.usermodel.Cell;

public class ExcelUtils {

    /**
     * 获取对象的String值
     *
     * @param value Object
     * @return String
     */
    public static String getValueFromRecord(Object value) {

        if (value instanceof Cell) {/*如果Record是一个Excel单元格*/
            Cell cell = (Cell) value;
            int cellType = cell.getCellType();

            switch (cellType) {
                case Cell.CELL_TYPE_NUMERIC:
                    value = String.valueOf(cell.getNumericCellValue());
                    break;
                case Cell.CELL_TYPE_STRING:
                    value = cell.getRichStringCellValue();
                    break;
                case Cell.CELL_TYPE_FORMULA:
                    value = cell.getCellFormula();
                    break;
                case Cell.CELL_TYPE_BLANK:
                    value = "";
                    break;
                case Cell.CELL_TYPE_BOOLEAN:
                    value = String.valueOf(cell.getBooleanCellValue());
                    break;
                case Cell.CELL_TYPE_ERROR:
                    value = String.valueOf(cell.getErrorCellValue());
                    break;
            }
        } else {
            // 如果是其他类型 ...
        }

        return String.valueOf(value);
        
    }

    /**
     * 获取Excel单元格的String值;
     *
     * @param cell ExcelCell
     * @return String
     */
    public static String getValueFromCell(Cell cell) {
        return getValueFromRecord(cell);
    }

}
