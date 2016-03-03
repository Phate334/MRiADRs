package edu.nuk.iadrs.data;

import java.text.ParseException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class Field {
	public static int AGE = 0;
	public static int SEX = 1;
	public static int DATE_MONTH = 2;
	public static int DRUG = 3;
	public static int PT = 4;
	
	public static String[] FIELD_NAME = {"AGE", "SEX", "DATE_MONTH", "DRUG", "PT"};
	
	public static int[][] CUBE_TYPE = {
		{AGE}, {SEX}, {DRUG}, {PT}, 
		{AGE, SEX}, {AGE, DRUG}, {AGE, PT}, {SEX, DRUG}, {SEX, PT}, {DRUG, PT}, 
		{AGE, SEX, DRUG}, {AGE, SEX, PT}, {AGE, DRUG, PT}, {SEX, DRUG, PT}, 
		{AGE, SEX, DRUG, PT}, {}
	};
	
	private String[] rowData = null;
	
	public Field(String line) throws ParseException{
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
	
	public Text getKey(int[] type)
	{
		Gson gson = new GsonBuilder().create();
		PassKey key = new PassKey();
		key.fieldType = Arrays.copyOf(type, type.length+1);
		key.fieldType[type.length] = DATE_MONTH;
		key.fieldValue = new String[type.length];
		for(int i = 0; i < type.length; i ++)
		{
			key.fieldValue[i] = rowData[type[i]];
		}
		return new Text(gson.toJson(key));
	}
}