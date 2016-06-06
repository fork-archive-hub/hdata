package com.github.stuxuhai.hdata.plugin.excel.reader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.github.stuxuhai.hdata.api.DefaultRecord;
import com.github.stuxuhai.hdata.api.Fields;
import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.OutputFieldsDeclarer;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Reader;
import com.github.stuxuhai.hdata.api.Record;
import com.github.stuxuhai.hdata.api.RecordCollector;
import com.github.stuxuhai.hdata.api.Splitter;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.github.stuxuhai.hdata.plugin.excel.ExcelProperties;
import com.google.common.base.Preconditions;

public class ExcelReader extends Reader {

	private Workbook workbook = null;
	private boolean includeColumnNames = false;
	private Fields fields = new Fields();

	@Override
	public void prepare(JobContext context, PluginConfig readerConfig) {
		String path = readerConfig.getString(ExcelProperties.PATH);
		Preconditions.checkNotNull(path, "Excel reader required property: path");

		try {
			if (path.endsWith(".xlsx")) {
				workbook = new XSSFWorkbook(new File(path));
			} else {
				workbook = new HSSFWorkbook(new FileInputStream(new File(path)));
			}
		} catch (InvalidFormatException | IOException e) {
			throw new HDataException(e);
		}

		includeColumnNames = readerConfig.getBoolean(ExcelProperties.INCLUDE_COLUMN_NAMES, false);
	}

	@Override
	public void execute(RecordCollector recordCollector) {
		if (workbook.getNumberOfSheets() > 0) {
			Sheet sheet = workbook.getSheetAt(0);

			if (includeColumnNames && sheet.getPhysicalNumberOfRows() > 0) {
				Row row = sheet.getRow(0);
				for (int cellIndex = row.getFirstCellNum(), cellLength = row
						.getPhysicalNumberOfCells(); cellIndex < cellLength; cellIndex++) {
					fields.add(row.getCell(cellIndex).toString());
				}
			}

			int startRow = includeColumnNames ? 1 : 0;
			for (int rowIndex = startRow, rowLength = sheet
					.getPhysicalNumberOfRows(); rowIndex < rowLength; rowIndex++) {
				Row row = sheet.getRow(rowIndex);
				Record record = new DefaultRecord(row.getPhysicalNumberOfCells());
				for (int cellIndex = row.getFirstCellNum(), cellLength = row
						.getPhysicalNumberOfCells(); cellIndex < cellLength; cellIndex++) {
					record.add(row.getCell(cellIndex).toString());
				}

				recordCollector.send(record);
			}
		}
	}

	@Override
	public void close() {
		if (workbook != null) {
			try {
				workbook.close();
			} catch (IOException e) {
				throw new HDataException(e);
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(fields);
	}

	@Override
	public Splitter newSplitter() {
		return null;
	}

}
