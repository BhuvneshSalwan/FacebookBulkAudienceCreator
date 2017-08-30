package com.google.bigquery.main;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.DatasetList;
import com.google.api.services.bigquery.model.DatasetList.Datasets;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableList;
import com.google.api.services.bigquery.model.TableList.Tables;

public class TableResults {

	public static final String PROJECT_ID = "stellar-display-145814";
	public static final String DATASET_ID = "table_output";
	public static final String TABLE_ID = "audience_create";

	public static TableDataList getResults(Bigquery bigquery) {

		try {
			TableDataList datalist = bigquery.tabledata().list(PROJECT_ID, DATASET_ID, TABLE_ID).execute();

			System.out.println("TOTAL ROWS : " + datalist.getTotalRows());
			
			return datalist;
		} catch (Exception e) {
			System.out.println("Exception : TableResults.class - getResults Method");
			System.out.println(e);
			return null;
		}

	}

	public static Boolean ListDataSet(Bigquery bigquery) {

		try {
			DatasetList datasets = bigquery.datasets().list(PROJECT_ID).execute();

			for (Datasets dataset : datasets.getDatasets()) {

				if (dataset.getId().equalsIgnoreCase(PROJECT_ID + ":" + DATASET_ID)) {
					System.out.println(dataset.getId());
					return true;
				}
			}

			return false;

		} catch (Exception e) {
			System.out.println("Exception : TableResults.class - ListDataSet");
			System.out.println(e);
			return false;
		}

	}

	public static Boolean ListTables(Bigquery bigquery) {

		try {
			TableList tables = bigquery.tables().list(PROJECT_ID, DATASET_ID).execute();

			for (Tables table : tables.getTables()) {

				if (table.getId().equalsIgnoreCase(PROJECT_ID + ":" + DATASET_ID + "." + TABLE_ID)) {
					System.out.println(table.getId());
					return true;
				}

			}

			return false;

		} catch (Exception e) {
			System.out.println("Exception : TableResults.class - ListDataSet");
			System.out.println(e);
			return false;
		}

	}

}