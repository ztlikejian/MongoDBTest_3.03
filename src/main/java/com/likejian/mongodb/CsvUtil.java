package com.likejian.mongodb;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import com.csvreader.CsvReader;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class CsvUtil {

	public static void main(String[] args) {
		try {
			CsvUtil csvUtil = new CsvUtil();
			System.out.println("读取文件开始");
			long time1 = System.currentTimeMillis();
			List<String[]> list = csvUtil.readCsv("D:\\test1.csv");
			System.out.println("读取文件结束");
			long time2 = System.currentTimeMillis();
			System.out.println("读取文件耗时：" + (time2 - time1));
			System.out.println("文件总条数：" + list.size());
			List<DBObject> dbObjectList = new ArrayList<DBObject>();
			DBObject user = null;
			for (int i = 0; i < list.size(); i++) {
				for(int j = 0; j < list.get(i).length; j++){
					user = new BasicDBObject();
					user.put("col_" + j, list.get(i)[j]);
				}
				dbObjectList.add(user);
			}
			DynamicCollection dynamicCollTest = new DynamicCollection();
			dynamicCollTest.dynamicInsert("collection201306", dbObjectList);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// 读取csv文件
	public List<String[]> readCsv(String filePath) throws Exception {
		List<String[]> csvList = new ArrayList<String[]>();
		CsvReader reader = new CsvReader(filePath, ',', Charset.forName("GBK"));
		reader.readHeaders(); // 跳过表头 如果需要表头的话，不要写这句。
		while (reader.readRecord()) { // 逐行读入除表头的数据
			csvList.add(reader.getValues());
		}
		reader.close();
		return csvList;
	}

	// 判断是否是csv文件
	private boolean isCsv(String fileName) {
		return fileName.matches("^.+\\.(?i)(csv)$");
	}
}
