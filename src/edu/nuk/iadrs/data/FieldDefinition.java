package edu.nuk.iadrs.data;

public class FieldDefinition {
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
}
