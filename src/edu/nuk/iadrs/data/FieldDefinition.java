package edu.nuk.iadrs.data;

public class FieldDefinition {
	public static int AGE = 0;
	public static int SEX = 1;
	public static int DRUG = 2;
	public static int PT = 3;
	public static int DATE_MONTH = 4;
	public static int ALL = 5;
	
	public static String[] FIELD_NAME = {"AGE", "SEX", "DRUG", "PT", "DATE_MONTH", "ALL"};
	
	public static int[][] CUBE_TYPE = {
		{AGE}, {SEX}, {DRUG}, {PT}, 
		{AGE, SEX}, {AGE, DRUG}, {AGE, PT}, {SEX, DRUG}, {SEX, PT}, {DRUG, PT}, 
		{AGE, SEX, DRUG}, {AGE, SEX, PT}, {AGE, DRUG, PT}, {SEX, DRUG, PT}, 
		{AGE, SEX, DRUG, PT}, {ALL}
	};
	public static int getTypeLength()
	{
		return CUBE_TYPE.length;
	}
	public static int[] getCubeType(int index)
	{
		if(index>=0 && index<CUBE_TYPE.length)
		{
			return CUBE_TYPE[index];
		}
		return null;
	}
	
	public static String getFieldName(int index)
	{
		if(index>=0 && index<CUBE_TYPE.length)
		{
			return FIELD_NAME[index];
		}
		return null;
	}
}
