package edu.nuk.iadrs.data;

import java.util.ArrayList;

public class FieldDefinition {
	public static int AGE = 0;
	public static int SEX = 1;
	public static int DRUG = 2;
	public static int PT = 3;
	public static int DATE_MONTH = 4;
	public static int ALL = 5;

	public static String[] FIELD_NAME = { "AGE", "SEX", "DRUG", "PT",
			"DATE_MONTH", "ALL" };

	public static int[][] CUBE_TYPE = { { AGE }, { SEX }, { DRUG }, { PT },
			{ AGE, SEX }, { AGE, DRUG }, { AGE, PT }, { SEX, DRUG },
			{ SEX, PT }, { DRUG, PT }, { AGE, SEX, DRUG }, { AGE, SEX, PT },
			{ AGE, DRUG, PT }, { SEX, DRUG, PT }, { AGE, SEX, DRUG, PT },
			{ ALL } };

	public static String[][] FULL_CUBE_TYPE = {
			{ "DRUG_PT", "DRUG", "PT", "ALL" },
			{ "AGE_DRUG_PT", "AGE_DRUG", "AGE_PT", "AGE" },
			{ "SEX_DRUG_PT", "SEX_DRUG", "SEX_PT", "SEX" },
			{ "AGE_SEX_DRUG_PT", "AGE_SEX_DRUG", "AGE_SEX_PT", "AGE_SEX" } };

	public static int getTypeLength() {
		return CUBE_TYPE.length;
	}

	public static int[] getCubeType(int index) {
		if (index >= 0 && index < CUBE_TYPE.length) {
			return CUBE_TYPE[index];
		}
		return null;
	}

	public static String getFieldName(int index) {
		if (index >= 0 && index < CUBE_TYPE.length) {
			return FIELD_NAME[index];
		}
		return null;
	}

	/**
	 * 從資料字串取得晶體類型，找出空字串的欄位。
	 * 
	 * @param rowData
	 * @return int:cube type
	 */
	public static String getCubeTypeName(String rowData) {
		String[] temp = rowData.split("#");
		ArrayList<String> indexTemp = new ArrayList<String>();
		for (int i = 0; i < DATE_MONTH; i++) {
			if (!temp[i].equals("")) {
				indexTemp.add(getFieldName(i));
			}
		}
		if (indexTemp.isEmpty()) {
			return "ALL";
		} else {
			return String.join("_", indexTemp);
		}
	}
}
