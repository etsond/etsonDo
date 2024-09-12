import logging
import pandas as pd
import numpy as np
import json
import os
import datetime
from application.db.sql_client import (
    SqlClient,
)  # Placeholder for SQL client when DB connection is available
from application.db.query import Query  # Placeholder for SQL queries
from application.mapping import Mapping  # For reconciliation logic
from application.app_global import (
    AppGlobal,
)  # Global app config for database connection strings

# Setup logger for debugging and info messages
logger = logging.getLogger(__name__)


class JAWSCasemgrRecon(object):

    def execute_locally(self):
        """
        Reads the data from the Excel file and passes it to the analyze method.
        """
        logger.info("Starting local execution with Excel data for testing")

        # Load the Excel file with two sheets: 'Casemgr' and 'JAWS DB'
        df_sheet_multi = pd.read_excel(
            "data/jaws_bpm_recon_raw_data_case_mgr.xlsx",
            sheet_name=["Casemgr", "JAWS DB"],
            converters={"JOURNAL_ID": str, "BUSINESS_UNIQUE_ID": str},
        )

        # Extract data for both 'Casemgr' and 'JAWS DB' sheets
        casemgr_data = df_sheet_multi["Casemgr"]
        jaws_data = df_sheet_multi["JAWS DB"]

        logger.info("Read successful from Excel sheet")

        # Call the analyze method with the extracted data
        self.analyze_data(jaws_data, casemgr_data)

    def analyze_data(self, jaws_data, casemgr_data):
        """
        Analyze and compare data from JAWS and Case Manager.
        This method processes, merges the datasets, and highlights discrepancies.
        """
        logger.info("Analyzing data for reconciliation")

        # Process the data
        case_df, jaws_df = self.process_data(jaws_data, casemgr_data)

        # Merge the data for reconciliation
        merged_data = self.merge_data(case_df, jaws_df)

        # Compare the data to highlight discrepancies
        discrepancies = self.compare_data(merged_data)

        # Save the final analysis results
        temp_dir = AppGlobal.Config.get("Storage", "TempDirectory")
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)

        output_file = os.path.join(
            temp_dir,
            f"JAWSCasemgrRecon_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        )
        discrepancies.to_excel(output_file, index=False)
        logger.info(f"Reconciled data saved to {output_file}")
        print(discrepancies)  # For quick review

    def process_data(self, jaws_data, casemgr_data):
        """
        Process both JAWS and Case Manager data for analysis.
        """
        # Convert the extracted data into DataFrames for easier manipulation
        case_df = pd.DataFrame(casemgr_data)
        jaws_df = pd.DataFrame(jaws_data)

        # Process the 'CASE_DATA' column in the Case Manager data
        case_df["CASE_DATA"] = case_df["CASE_DATA"].apply(json.loads)
        case_df = case_df.explode("CASE_DATA")  # Explode JSON column into rows

        # Perform further data cleaning and column transformations
        case_df[["colNull", "Symbol", "nullcol", "Security ID"]] = case_df[
            "key"
        ].str.split("3", expand=True)
        case_df = case_df.rename(columns={"value": "Status"})
        case_df = case_df.drop(columns=["key", "colNull", "nullcol"])

        # Clean and standardize the JAWS data for matching
        case_df["Security ID"] = case_df["Security ID"].str.strip().str.upper()
        jaws_df["IDN_REQUEST"] = jaws_df["IDN_REQUEST"].str.strip().str.upper()

        return case_df, jaws_df

    def merge_data(self, case_df, jaws_df):
        """
        Merge JAWS and Case Manager data for comparison.
        """
        merged = pd.merge(
            case_df,
            jaws_df,
            left_on="Security ID",
            right_on="IDN_REQUEST",
            how="outer",
        )

        # Reconcile the statuses
        merged["Recon Status"] = np.vectorize(self.calreconstatuscase)(
            merged["Status"], merged["CDE_JNL_STA"]
        )

        return merged

    def compare_data(self, merged_data):
        """
        Compare the merged data and highlight any discrepancies.
        """
        # Compare statuses between Case Manager and JAWS
        discrepancies = merged_data[merged_data["Status"] != merged_data["CDE_JNL_STA"]]
        return discrepancies

    def calreconstatuscase(self, case_status, jaws_status):
        """
        Reconcile statuses between Case Manager and JAWS using a mapping logic.
        """
        map = Mapping()  # Utilize the mapping logic for reconciliation
        valid_jaws_sta = map.jaws_casestatus_map(jaws_status, case_status)
        return valid_jaws_sta

    def execute_full(self):
        """
        Full reconciliation process using SQL data from JAWS and (eventually) Case Manager.
        """
        logger.info("Starting full recon process with JAWS SQL database")

        # Connect to the JAWS SQL database
        jawsDBConn = AppGlobal.Config.get("Databases", "JAWSDBConnectionString")
        sqlConn = SqlClient(jawsDBConn)  # Connect to JAWS SQL database
        test_query = Query("Test_Sql")

        # Use a time-based filter to fetch recent data (last 30 minutes)
        updated_time = datetime.datetime.now() - datetime.timedelta(minutes=30)
        updated_time_tuple = tuple([updated_time.strftime("%Y-%m-%d %H:%M:%S")])

        logger.info("Executing query on JAWS DB")
        jaws_data = sqlConn.executeProcedure(test_query.getQuery(), updated_time_tuple)
        logger.info("Fetched data from JAWS DB")

        # Placeholder for connecting to Case Manager DB once the query is available
        casemgrDBConn = AppGlobal.Config.get("Databases", "CaseMgrDBConnectionString")
        case_sqlConn = SqlClient(casemgrDBConn)  # Connect to Case Manager SQL database
        case_query = Query("CaseMgr_Sql")

        # For now, continue using Excel data for Casemgr
        logger.info("Falling back to Excel data for Casemgr due to missing DB query")
        self.execute_locally()  # Reuse the local logic until SQL integration is complete
