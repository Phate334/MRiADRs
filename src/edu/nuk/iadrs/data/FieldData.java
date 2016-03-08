package edu.nuk.iadrs.data;

import java.text.ParseException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

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
	public Text getKey(IntWritable[] type)
	{
		//拆drug & PT
		String[] temp = new String[rowData.length];
		for(int i=0; i<type.length; i++)
		{
			temp[i] = rowData[type[i].get()];
		}
		temp[FieldDefinition.DATE_MONTH] = rowData[FieldDefinition.DATE_MONTH];
		for(int i=0; i<temp.length; i++)
		{
			if(temp[i] == null)
			{
				temp[i] = "";
			}
		}
		return new Text(String.join("#", temp));
	}
}