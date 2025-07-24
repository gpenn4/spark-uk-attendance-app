import pandas as pd
import sys
from pyspark.sql import SparkSession
from rich.console import Console

import queries, util

DATA_FILE = "./data/Absence_3term201819_nat_reg_la_sch.csv"
DATA_URL = "https://explore-education-statistics.service.gov.uk/data-catalogue/data-set/097fd311-d368-4a12-ac38-45efab3f3f95/csv"

def main_menu():
    console = Console()
    while True:
        console.print("\n=== School Absence Analysis Menu ===", style="bold magenta")
        console.print("1. Search enrolments by local authority.")
        console.print("2. Search authorised absences by school type.")
        console.print("3. Search unauthorised absences by region or local authority.")
        console.print("4. Compare 2 local authorities.")
        console.print("5. Explore performance of regions from 2006-2018.")
        console.print("6. Explore link between school type, location, and pupil absences.")
        console.print("7. Exit/ Quit \n")

        choice = input("Enter your choice (1-7): ").strip()

        if choice == "1":
            queries.search_enrolments(clean_data)
        elif choice == "2":
            queries.search_authorized_absences(clean_data)
        elif choice == "3":
            queries.search_unauthorized_absences(clean_data)
        elif choice == "4":
            queries.compare_two_authorities(clean_data)
        elif choice == "5":
            queries.performance_exploration(clean_data)
        elif choice == "6":
            queries.explore_links(clean_data, clean_data_school)
        elif choice == "7":
            console.print("\n Exiting...")
            break
        else:
            console.print("\nInvalid choice. Please enter a number between 1 and 7: ", style="bold red")
            # choice = input("\nInvalid choice. Please enter a number between 1 and 7: ")


if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("England School Absence Analysis")
        .getOrCreate()
    )

    # UNCOMMENT LINES 53-54 IF YOU DO NOT DOWNLOAD THE DATASET
    # df_pandas = pd.read_csv(DATA_URL, low_memory=False)
    # data = spark.createDataFrame(df_pandas)

    # COMMENT OUT LINES 57-59 IF YOU DO NOT DOWNLOAD THE DATASET
    data = (
        spark.read.options(header=True, inferSchema=True).format("csv").load(DATA_FILE)
    )

    clean_data_school = util.preprocess_data(data, True)
    clean_data = util.preprocess_data(data, False)

    main_menu()
