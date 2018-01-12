package com.github.stuxuhai.hdata.plugin.excel.writer;

import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.github.stuxuhai.hdata.api.Fields;
import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Record;
import com.github.stuxuhai.hdata.api.Writer;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.github.stuxuhai.hdata.plugin.excel.ExcelProperties;
import com.google.common.base.Preconditions;

public class ExcelWriter extends Writer {

	private String path = null;
	private boolean includeColumnNames = true;
	private Workbook workbook = null;
	private Sheet sheet = null;
	private int rowIndex = 0;

	@Override
	public void prepare(JobContext context, PluginConfig writerConfig) {
		path = writerConfig.getString(ExcelProperties.PATH);
		Preconditions.checkNotNull(path, "Excel writer required property: path");

		includeColumnNames = writerConfig.getBoolean(ExcelProperties.INCLUDE_COLUMN_NAMES, false);

		workbook = new XSSFWorkbook();
		sheet = workbook.createSheet("工作表1");

		if (includeColumnNames) {
			Fields fields = context.getFields();
			if (fields != null && fields.size() > 0) {
				Row row = sheet.createRow(rowIndex++);
				for (int i = 0, len = fields.size(); i < len; i++) {
					Cell cell = row.createCell(i);
					cell.setCellType(XSSFCell.CELL_TYPE_STRING);
					Object value = fields.get(i);
					XSSFRichTextString content = new XSSFRichTextString(value != null ? value.toString() : null);
					cell.setCellValue(content);
				}
			}
		}
	}

	@Override
	public void execute(Record record) {
		Row row = sheet.createRow(rowIndex++);
		for (int i = 0, len = record.size(); i < len; i++) {
			Cell cell = row.createCell(i);
			cell.setCellType(XSSFCell.CELL_TYPE_STRING);
			Object value = record.get(i);
			XSSFRichTextString content = new XSSFRichTextString(value != null ? value.toString() : null);
			cell.setCellValue(content);
		}
	}

	@Override
	public void close() {
		if (workbook != null) {
			try {
				FileOutputStream fos = new FileOutputStream(path);
				workbook.write(fos);
				fos.flush();
				fos.close();
				workbook.close();
			} catch (IOException e) {
				throw new HDataException(e);
			}
		}
	}
}
