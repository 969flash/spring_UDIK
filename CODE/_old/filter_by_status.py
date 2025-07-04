import os
import pandas as pd


def filter_and_process_csv(input_csv_folder, output_csv_folder):
    def calculate_survival(row):
        try:
            return (
                True
                if row["영업상태명"] == "영업/정상"
                or (
                    row["영업상태명"] == "폐업"
                    and int(row["폐업일자"].split("-")[0]) > 2019
                )
                else False
            )
        except Exception as e:
            print(f"Error processing row: {row}")
            raise e

    # Ensure the output directory exists
    os.makedirs(output_csv_folder, exist_ok=True)

    # Iterate through all CSV files in the input directory
    for file_name in os.listdir(input_csv_folder):
        if file_name.endswith(".csv"):
            # Check the number of rows in the CSV file
            file_path = os.path.join(input_csv_folder, file_name)

            # Read the CSV file
            df = pd.read_csv(file_path)

            # 데이터가 50개 이하인 csv파일은 제외.
            if len(df) <= 50:
                continue

            # Filter rows where "영업상태명" is either "폐업" or "영업/정상"
            filtered_df = df[df["영업상태명"].isin(["폐업", "영업/정상"])]

            # Filter rows where "인허가일자" is in the year 2016
            filtered_df = filtered_df[
                filtered_df["인허가일자"].str.split("-").str[0] == "2016"
            ]

            # 영업 상태명이 폐업이지만 폐업일자가 없는경우 필터링
            filtered_df = filtered_df[
                ~(
                    (filtered_df["영업상태명"] == "폐업")
                    & (filtered_df["폐업일자"].isnull())
                )
            ]

            # 데이터가 50개 이하인 csv파일은 제외.
            if len(filtered_df) <= 50:
                continue

            # Create a new column "생존" with 0 or 1 based on the conditions
            filtered_df["생존"] = filtered_df.apply(calculate_survival, axis=1)

            # Save the filtered DataFrame to the output directory
            output_path = os.path.join(output_csv_folder, file_name)
            filtered_df.to_csv(output_path, index=False)
