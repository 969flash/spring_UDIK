import os
import pandas as pd
import numpy as np
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from scipy.spatial import KDTree
import traceback


def add_adj_industry_count_column(input_csv_folder, output_csv_folder):
    # type: (str, str) -> str
    output_csv_folder = os.path.join(output_csv_folder + "/" + "04adj_industry_added")
    os.makedirs(output_csv_folder, exist_ok=True)

    def process_file(file_name):
        print(f"Processing {file_name}...")
        try:
            input_file_path = os.path.join(input_csv_folder, file_name)
            output_file_path = os.path.join(output_csv_folder, file_name)

            # Read the CSV file
            df = pd.read_csv(input_file_path)

            # Calculate nearest neighbor distances
            coords = df[["좌표정보x(epsg5174)", "좌표정보y(epsg5174)"]].values
            if len(coords) < 2:
                df["adj_industry_count"] = np.zeros(len(df), dtype=int)
            else:
                tree = KDTree(coords)
                avg_distance = df[
                    "동일업종 최근접거리의 평균"
                ].mean()  # Mean of all values in the column "동일업종 최근접거리의 평균"
                df["adj_industry_count"] = [
                    len(tree.query_ball_point(coord, avg_distance)) - 1
                    for coord in coords
                ]

            # Save the updated DataFrame to the output folder
            df.to_csv(output_file_path, index=False)
            print(f"Processed and saved: {file_name}")

        except Exception as e:
            print(f"Error processing {file_name}: {e}")
            traceback.print_exc()

    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor() as executor:
        csv_files = [f for f in os.listdir(input_csv_folder) if f.endswith(".csv")]
        executor.map(process_file, csv_files)

    return output_csv_folder


if __name__ == "__main__":
    OUTPUT_FOLDER = (
        "C:/Users/bsh96/Documents/GitHub/spring_UDIK/DATA/LOCALDATA_FILTERED_CSV"
    )

    input_csv_folder = "C:/Users/bsh96/Documents/GitHub/spring_UDIK/DATA/LOCALDATA_FILTERED_CSV/03DISTANCE_CALCULATED"

    # 생존 column 추가
    output_csv_folder = add_adj_industry_count_column(
        input_csv_folder,
        OUTPUT_FOLDER,
    )
