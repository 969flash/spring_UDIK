import os
import pandas as pd
import numpy as np
import traceback


def get_total_chart(input_csv_folder, ouput_folder):
    try:
        # Create output folder if it doesn't exist
        if not os.path.exists(ouput_folder):
            os.makedirs(ouput_folder)

        # Initialize a dictionary to store counts
        industry_counts = {}

        # Read all CSV files in the input folder
        for file_name in os.listdir(input_csv_folder):
            print("IMPORT FILE: ", file_name)
            if file_name.endswith(".csv"):
                file_path = os.path.join(input_csv_folder, file_name)
                df = pd.read_csv(file_path)
                industry_name = file_name.split("_")[-1]
                industry_name = industry_name.replace(".csv", "")
                industry_counts[industry_name] = len(df)

        # Calculate total permits
        total_permits = sum(industry_counts.values())

        # Prepare data for output
        output_data = []
        for industry, count in industry_counts.items():
            percentage = (count / total_permits) * 100
            print(industry, count, percentage)
            output_data.append(
                {"업종": industry, "인허가수": count, "퍼센트": percentage}
            )

        # Convert to DataFrame and save to CSV
        output_df = pd.DataFrame(output_data)
        output_csv_path = os.path.join(ouput_folder, "업종별_인허가수_분석.csv")
        output_df.to_csv(output_csv_path, index=False, encoding="utf-8-sig")

        return ouput_folder

    except Exception as e:
        print("An error occurred:", e)
        traceback.print_exc()
        return None


def get_survival_ratio_by_distance(input_csv_folder, ouput_folder):
    try:
        # Create output folder if it doesn't exist
        if not os.path.exists(ouput_folder):
            os.makedirs(ouput_folder)

        # Initialize a dictionary to store survival counts
        survival_counts = {}

        # Read all CSV files in the input folder
        for file_name in os.listdir(input_csv_folder):
            print("IMPORT FILE: ", file_name)
            if file_name.endswith(".csv"):
                file_path = os.path.join(input_csv_folder, file_name)
                df = pd.read_csv(file_path)
                industry_name = file_name.split("_")[-1]
                industry_name = industry_name.replace(".csv", "")

                # Calculate survival rate
                total_count = len(df)
                survival_count = df["생존"].sum() if "생존" in df.columns else 0
                survival_rate = (
                    (survival_count / total_count) * 100 if total_count > 0 else 0
                )
                # 거리평균
                distance_average = (
                    df["동일업종 최근접거리의 평균"].mean()
                    if "동일업종 최근접거리의 평균" in df.columns
                    else 0
                )
                # 거리표준편차
                distance_std = (
                    df["동일업종 최근접거리의 평균"].std()
                    if "동일업종 최근접거리의 평균" in df.columns
                    else 0
                )
                # 거리최대값
                distance_max = (
                    df["동일업종 최근접거리의 평균"].max()
                    if "동일업종 최근접거리의 평균" in df.columns
                    else 0
                )
                # 거리최소값
                distance_min = (
                    df["동일업종 최근접거리의 평균"].min()
                    if "동일업종 최근접거리의 평균" in df.columns
                    else 0
                )

                survival_counts[industry_name] = {
                    "total": total_count,
                    "survival": survival_count,
                    "rate": round(100 - survival_rate, 2),
                    "distance_average": round(distance_average, 2),
                    "distance_std": round(distance_std, 2),
                    "distance_max": round(distance_max, 2),
                    "distance_min": round(distance_min, 2),
                }

        # Prepare data for output
        output_data = []
        for industry, data in survival_counts.items():
            print(industry, data["total"], data["survival"], data["rate"])
            output_data.append(
                {
                    "업종": industry,
                    "총 개수": data["total"],
                    "생존 개수": data["survival"],
                    "개업 이후 3년 내 폐업률(%)": data["rate"],
                    "동일업종 최근접거리의 평균": data["distance_average"],
                    "동일업종 최근접거리의 평균_표준편차": data["distance_std"],
                    "동일업종 최근접거리의 평균_최대값": data["distance_max"],
                    "동일업종 최근접거리의 평균_최소값": data["distance_min"],
                }
            )

        # Convert to DataFrame and save to CSV
        output_df = pd.DataFrame(output_data)
        output_csv_path = os.path.join(ouput_folder, "업종별_폐업율_분석.csv")
        output_df.to_csv(output_csv_path, index=False, encoding="utf-8-sig")

        return ouput_folder

    except Exception as e:
        print("An error occurred:", e)
        traceback.print_exc()
        return None


def get_survival_ratio_by_adj_count(input_csv_folder, ouput_folder):
    try:
        # Create output folder if it doesn't exist
        if not os.path.exists(ouput_folder):
            os.makedirs(ouput_folder)

        # Initialize a dictionary to store survival counts
        survival_counts = {}

        # Read all CSV files in the input folder
        for file_name in os.listdir(input_csv_folder):
            print("IMPORT FILE: ", file_name)
            if file_name.endswith(".csv"):
                file_path = os.path.join(input_csv_folder, file_name)
                df = pd.read_csv(file_path)
                industry_name = file_name.split("_")[-1]
                industry_name = industry_name.replace(".csv", "")

                # Calculate survival rate
                total_count = len(df)
                survival_count = df["생존"].sum() if "생존" in df.columns else 0
                survival_rate = (
                    (survival_count / total_count) * 100 if total_count > 0 else 0
                )
                # 인접 동일 업장 수 평균
                adj_count_average = (
                    df["adj_industry_count"].mean()
                    if "adj_industry_count" in df.columns
                    else 0
                )
                # 인접 동일 업장 수 표준편차
                adj_count_std = (
                    df["adj_industry_count"].std()
                    if "adj_industry_count" in df.columns
                    else 0
                )
                # 인접 동일 업장 수 최대값
                adj_count_max = (
                    df["adj_industry_count"].max()
                    if "adj_industry_count" in df.columns
                    else 0
                )
                # 인접 동일 업장 수 최소값
                adj_count_min = (
                    df["adj_industry_count"].min()
                    if "adj_industry_count" in df.columns
                    else 0
                )

                survival_counts[industry_name] = {
                    "total": total_count,
                    "survival": survival_count,
                    "rate": round(100 - survival_rate, 2),
                    "adj_count_average": round(adj_count_average, 2),
                    "adj_count_std": round(adj_count_std, 2),
                    "adj_count_max": round(adj_count_max, 2),
                    "adj_count_min": round(adj_count_min, 2),
                }

        # Prepare data for output
        output_data = []
        for industry, data in survival_counts.items():
            print(industry, data["total"], data["survival"], data["rate"])
            output_data.append(
                {
                    "업종": industry,
                    "총 개수": data["total"],
                    "생존 개수": data["survival"],
                    "개업 이후 3년 내 폐업률(%)": data["rate"],
                    "인접 업장수의 평균": data["adj_count_average"],
                    "인접 업장수의 표준편차": data["adj_count_std"],
                    "인접 업장수의 최대값": data["adj_count_max"],
                    "인접 업장수의 최소값": data["adj_count_min"],
                }
            )

        # Convert to DataFrame and save to CSV
        output_df = pd.DataFrame(output_data)
        output_csv_path = os.path.join(
            ouput_folder, "업종별_폐업율_분석_인접업종수.csv"
        )
        output_df.to_csv(output_csv_path, index=False, encoding="utf-8-sig")

        return ouput_folder

    except Exception as e:
        print("An error occurred:", e)
        traceback.print_exc()
        return None


if __name__ == "__main__":
    OUTPUT_FOLDER = (
        "C:/Users/bsh96/Documents/GitHub/spring_UDIK/DATA/LOCALDATA_FILTERED_CSV"
    )

    # input_csv_folder = "C:/Users/bsh96/Documents/GitHub/spring_UDIK/DATA/LOCALDATA_FILTERED_CSV/01filtered"
    # # 업종별 인허가수 분석
    # output_csv_folder = get_total_chart(
    #     input_csv_folder,
    #     OUTPUT_FOLDER,
    # )

    # input_csv_folder = "C:/Users/bsh96/Documents/GitHub/spring_UDIK/DATA/LOCALDATA_FILTERED_CSV/03DISTANCE_CALCULATED"
    # # 업종별 인허가수 분석
    # output_csv_folder = get_survival_ratio_by_distance(
    #     input_csv_folder,
    #     OUTPUT_FOLDER,
    # )

    input_csv_folder = "C:/Users/bsh96/Documents/GitHub/spring_UDIK/DATA/LOCALDATA_FILTERED_CSV/04adj_industry_added"
    # 업종별 인허가수 분석
    output_csv_folder = get_survival_ratio_by_adj_count(
        input_csv_folder,
        OUTPUT_FOLDER,
    )
