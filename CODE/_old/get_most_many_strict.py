import os
import pandas as pd
from collections import Counter

### 인허가 건수가 제일 많은 구를 구해 연구할 구를 선정한다.


# Define the path to the SEOUL folder
folder_path = "C:/Users/bsh96/Desktop/2025-1/춘계학술대회/DATA/FILTERED_CSV/SEOUL"


def count_businesses_by_district(folder_path):
    # Initialize a Counter to count the number of businesses per district
    district_counter = Counter()

    # Iterate through all CSV files in the folder
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".csv"):
            file_path = os.path.join(folder_path, file_name)
            # Read the CSV file
            try:
                data = pd.read_csv(file_path, encoding="utf-8")
            except UnicodeDecodeError:
                data = pd.read_csv(file_path, encoding="cp949")

            # Extract the "소재지전체주소" column
            if "소재지전체주소" in data.columns:
                addresses = data["소재지전체주소"].dropna()
                for address in addresses:
                    if address.startswith("서울특별시"):
                        # Extract the district name
                        district = address.split(" ")[1]
                        district_counter[district] += 1

    if not district_counter:
        print("서울특별시 데이터를 찾을 수 없습니다.")
        exit()

    # Remove districts with 20 or fewer businesses
    district_counter = Counter(
        {district: count for district, count in district_counter.items() if count > 100}
    )
    # Print the number of businesses for each district
    print("각 구별 업장 인허가 수:")
    for district, count in district_counter.items():
        print(f"{district}: {count}개")

    # Find the district with the most businesses
    if district_counter:
        most_common_district, most_common_count = district_counter.most_common(1)[0]
        print(
            f"\n업장 인허가 수가 가장 많은 구: {most_common_district} ({most_common_count}개)"
        )

    return district_counter


# Call the function
district_counter = count_businesses_by_district(folder_path)
