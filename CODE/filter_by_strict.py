import os
import pandas as pd

# Define the path to the SEOUL folder
folder_path = "C:/Users/bsh96/Desktop/2025-1/춘계학술대회/DATA/FILTERED_CSV/SEOUL"

# Create a folder for filtered data
output_folder = os.path.join(folder_path, "GANGNAM")
os.makedirs(output_folder, exist_ok=True)


def filter_gangnam_data():
    # Iterate through all CSV files in the folder
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".csv"):
            file_path = os.path.join(folder_path, file_name)
            # Read the CSV file
            try:
                data = pd.read_csv(file_path, encoding="utf-8")
            except UnicodeDecodeError:
                data = pd.read_csv(file_path, encoding="cp949")

            # Filter rows where "소재지전체주소" contains "서울특별시 강남구"
            if "소재지전체주소" in data.columns:
                gangnam_data = data[
                    data["소재지전체주소"].str.contains("서울특별시 강남구", na=False)
                ]

                # Save the filtered data to the GANGNAM folder
                output_file_path = os.path.join(output_folder, file_name)
                gangnam_data.to_csv(output_file_path, index=False, encoding="utf-8")


# Execute the function
filter_gangnam_data()
print(f"강남구 데이터가 {output_folder} 폴더에 저장되었습니다.")
