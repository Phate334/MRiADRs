package edu.nuk.iadrs;

import java.text.ParseException;

public class Field {
	public static int AGE = 0;
	public static int SEX = 1;
	public static int DATE_MONTH = 2;
	public static int DRUG = 3;
	public static int PT = 4;
	
	private String[] row = null;
	
	public Field(String line) throws ParseException{
		row = line.split("#");
		if(row.length != 5)
		{
				throw new ParseException("not enough field.", row.length);
		}
	}
	public String get(int field)
	{
		return row[field];
	}
}
