package com.qianjiali2.hiveDependency.dataMap.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Date;

import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import com.qianjiali.hiveDependency.entity.ParseNode;

public class ExcelUtils2 {

	private static HSSFWorkbook wb = null;

	private static void CreateExcel(String FileName) throws Exception {
		try {
			File file = new File(FileName);
			if (file.exists() && file.isFile()) {
				file.delete();
			}
			FileOutputStream fileOut = new FileOutputStream(file);
			wb = new HSSFWorkbook();
			HSSFSheet sheet = wb.createSheet("Sheet_1");
			HSSFRow row = sheet.createRow(0);
			row.createCell(0).setCellValue("ScriptName");
			row.createCell(1).setCellValue("SourceTableName");
			row.createCell(2).setCellValue("TargetTableName");
			row.createCell(3).setCellValue("Operate");
			wb.write(fileOut);
		} catch (Exception e) {
			wb = null;
			throw e;
		}
	}

	public static void addCells(String ResultFile, String FileName, String SourceTable, String TargetTable,
			String operate) throws Exception {
		try {
			if (wb == null) {
				CreateExcel(ResultFile);
			}
			HSSFSheet sheet = wb.getSheetAt(0);// 获取到工作表，因为一个excel可能有多个工作表  
			HSSFRow row = sheet.getRow(0);// 获取第一行（excel中的行默认从0开始，所以这就是为什么，一个excel必须有字段列头），即，字段列头，便于赋值  
			FileOutputStream out = new FileOutputStream(ResultFile); // 向d://test.xls中写数据  
			row = sheet.createRow((short) (sheet.getLastRowNum() + 1)); // 在现有行号后追加数据  
			row.createCell(0).setCellValue(FileName); // 设置第一个（从0开始）单元格的数据  
			row.createCell(1).setCellValue(SourceTable); // 设置第二个（从0开始）单元格的数据 
			row.createCell(2).setCellValue(TargetTable); // 设置第二个（从0开始）单元格的数据  
			row.createCell(3).setCellValue(operate); // 设置第二个（从0开始）单元格的数据  
			out.flush();
			wb.write(out);
			out.close();
			wb.close();
		} catch (Exception e) {
			throw e;
		}
	}
}
