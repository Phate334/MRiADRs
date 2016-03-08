package edu.nuk.iadrs.data;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;

public class FieldData {
	private String[] rowData = null;
	
	public FieldData(String line) throws ParseException{
		rowData = line.split("#");
		if(rowData.length != 5)
		{
			throw new ParseException("not enough field.", rowData.length);
		}
		
	}
	
	public String getRowValue(int field)
	{
		try{
			return rowData[field];
		}
		catch(ArrayIndexOutOfBoundsException e){
			return null;
		}
	}
	/**
	 * 依照需求挑出欄位回傳Text。
	 * @param type，需要的欄位index，定義在FieldDefinition中。
	 * @return Text，hadoop writable instance。
	 */
	public ArrayList<String> getKey(IntWritable[] type)
	{
		//挑出需要的欄位
		String[] temp = new String[rowData.length];
		for(int i=0; i<type.length; i++)
		{
			temp[type[i].get()] = rowData[type[i].get()];
		}
		temp[FieldDefinition.DATE_MONTH] = rowData[FieldDefinition.DATE_MONTH];
		for(int i=0; i<temp.length; i++)
		{
			if(temp[i] == null)
			{
				temp[i] = "";
			}
		}
		return splitDrugPt(temp);
	}
	/**
	 * 拆開drug和PT欄位的資料，假如需要的話。
	 * @return ArrayList<String>
	 */
	public ArrayList<String> splitDrugPt(String[] rowData)
	{
		ArrayList<String> outKeys = new ArrayList<String>();
		if(rowData[FieldDefinition.DRUG].equals("") && rowData[FieldDefinition.PT].equals(""))  //兩個欄位都不需要
		{
			outKeys.add(String.join("#", rowData));
		}
		else if(!rowData[FieldDefinition.DRUG].equals("") && rowData[FieldDefinition.PT].equals(""))  //只要drug
		{
			String[] temp = Arrays.copyOf(rowData, rowData.length);
			for(String drug: rowData[FieldDefinition.DRUG].split("\\$"))
			{
				temp[FieldDefinition.DRUG] = drug;
				outKeys.add(String.join("#", temp));
			}
		}
		else if(rowData[FieldDefinition.DRUG].equals("") && !rowData[FieldDefinition.PT].equals(""))  //只要PT
		{
			String[] temp = Arrays.copyOf(rowData, rowData.length);
			for(String pt: rowData[FieldDefinition.PT].split("\\$"))
			{
				temp[FieldDefinition.PT] = pt;
				outKeys.add(String.join("#", temp));
			}
		}
		else if(!rowData[FieldDefinition.DRUG].equals("") && !rowData[FieldDefinition.PT].equals(""))  //兩種欄位都要
		{
			String[] temp = Arrays.copyOf(rowData, rowData.length);
			for(String drug: rowData[FieldDefinition.DRUG].split("\\$"))
			{
				temp[FieldDefinition.DRUG] = drug;
				for(String pt: rowData[FieldDefinition.PT].split("\\$"))
				{
					temp[FieldDefinition.PT] = pt;
					outKeys.add(String.join("#", temp));
				}
			}
		}
		return outKeys;
	}
}