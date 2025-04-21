import os
import pandas as pd
import numpy as np
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from scipy.spatial import KDTree
import traceback


def filter_by_address(input_csv_folder, output_csv_folder, address_name="서울특별시"):
    # type: (str, str, str) -> str
    print("=============================")
    print("===== filter_by_address =====")
    print("=============================")

    # Output folder 이름 설정
    output_csv_folder = os.path.join(output_csv_folder + "/" + address_name)

    # Output folder 생성
    if not os.path.exists(output_csv_folder):
        os.makedirs(output_csv_folder)

    # MERGED_CSV 폴더 내의 모든 CSV 파일 처리
    for file_name in os.listdir(input_csv_folder):
        print("Filtering by address_name -> file_name: ", file_name)
        if file_name.endswith(".csv"):
            file_path = os.path.join(input_csv_folder, file_name)

            # CSV 파일 읽기
            try:
                df = pd.read_csv(file_path, encoding="utf-8-sig", on_bad_lines="warn")
            except UnicodeDecodeError:
                df = pd.read_csv(file_path, encoding="cp949", on_bad_lines="warn")
            # "소재지전체주소" 열에서 city이 포함된 데이터 필터링
            if "소재지전체주소" in df.columns:
                filtered_df = df[
                    df["소재지전체주소"].str.contains(address_name, na=False)
                ]

                # 필터링된 데이터를 output_csv_folder 저장
                output_path = os.path.join(output_csv_folder, file_name)
                filtered_df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(
        f"Filtered data containing '{address_name}' has been saved to {output_csv_folder}."
    )
    return output_csv_folder


def filter_by_open_and_close(input_csv_folder, output_csv_folder, open_year):
    # type: (str, str, str) -> str
    print("=============================")
    print("= filter_by_open_and_close ==")
    print("=============================")

    # Output folder 이름 설정
    output_csv_folder = os.path.join(output_csv_folder + "/00openat" + str(open_year))

    # Output folder 생성
    if not os.path.exists(output_csv_folder):
        os.makedirs(output_csv_folder)

    for file_name in os.listdir(input_csv_folder):
        print("filter_by_open_and_close -> file_name: ", file_name)
        # Check the number of rows in the CSV file
        file_path = os.path.join(input_csv_folder, file_name)

        # Read the CSV file
        df = pd.read_csv(file_path, encoding="utf-8-sig", on_bad_lines="warn")

        # Filter rows where "영업상태명" is either "폐업" or "영업/정상"
        filtered_df = df[df["영업상태명"].isin(["폐업", "영업/정상"])]

        # Filter rows where "인허가일자" is in the year 2016
        filtered_df = filtered_df[
            filtered_df["인허가일자"].str.split("-").str[0] == "2016"
        ]

        # 영업 상태명이 폐업이지만 폐업일자가 없는경우 제거
        filtered_df = filtered_df[
            ~(
                (filtered_df["영업상태명"] == "폐업")
                & (filtered_df["폐업일자"].isnull())
            )
        ]
        filtered_df.to_csv(
            os.path.join(output_csv_folder, file_name),
            index=False,
            encoding="utf-8-sig",
        )

    print(
        f"Filtered data containing '{"OPENAND CLOSE"}' has been saved to {output_csv_folder}."
    )
    return output_csv_folder


def add_survival_column(input_csv_folder, output_csv_folder):
    # type: (str, str) -> str
    def calculate_survival(row):
        # 개업 이후부터 만 3년을 채운 경우는 True (생존) 그렇지 못한 경우는 False (사망)
        if row["영업상태명"] == "영업/정상":
            return True

        # 폐업하였더라도 3년이상 영업한경우는 생존으로 간주
        if row["영업상태명"] == "폐업":
            start_date = datetime.strptime(row["인허가일자"], "%Y-%m-%d")
            end_date = datetime.strptime(row["폐업일자"], "%Y-%m-%d")
            survival_period = (end_date - start_date).days / 365.0

            return survival_period > 3

        return False

        # try:
        #     return (
        #         True
        #         if row["영업상태명"] == "영업/정상"
        #         or (
        #             row["영업상태명"] == "폐업"
        #             and int(row["폐업일자"].split("-")[0]) > 2019
        #         )
        #         else False
        #     )
        # except Exception as e:
        #     print(f"Error processing row: {row}")
        #     raise e

    print("=============================")
    print("==== add_survival_column ====")
    print("=============================")

    # Output folder 이름 설정
    output_csv_folder = os.path.join(output_csv_folder + "/02survival")

    # Ensure the output directory exists
    os.makedirs(output_csv_folder, exist_ok=True)

    # Iterate through all CSV files in the input directory
    print("input_csv_folder: ", input_csv_folder)
    for file_name in os.listdir(input_csv_folder):
        print("add_survival_column -> file_name: ", file_name)
        # Check the number of rows in the CSV file
        file_path = os.path.join(input_csv_folder, file_name)

        # Read the CSV file
        df = pd.read_csv(file_path, encoding="utf-8-sig", on_bad_lines="warn")

        # Create a new column "생존" with 0 or 1 based on the conditions
        df["생존"] = df.apply(calculate_survival, axis=1)

        # Save the filtered DataFrame to the output directory
        output_path = os.path.join(output_csv_folder + "/" + file_name)
        df.to_csv(output_path, index=False)

    return output_csv_folder


def filter_small_csv_files(input_csv_folder, output_csv_folder, max_rows=50):
    # type: (str, str, int) -> str
    print("=============================")
    print("== filter_small_csv_files ===")
    print("=============================")

    # Output folder 이름 설정
    output_csv_folder = os.path.join(output_csv_folder + "/01filtered")

    # Ensure the output directory exists
    os.makedirs(output_csv_folder, exist_ok=True)

    # Iterate through all CSV files in the input directory
    for file_name in os.listdir(input_csv_folder):
        print("filter_small_csv_files -> file_name: ", file_name)
        file_path = os.path.join(input_csv_folder, file_name)

        # Read the CSV file
        df = pd.read_csv(file_path, encoding="utf-8-sig", on_bad_lines="warn")

        # 데이터 50개 이상인 파일만 남긴다.
        if len(df) > max_rows:
            # Save the file to the output directory
            output_path = os.path.join(output_csv_folder + "/" + file_name)
            df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(
        f"CSV files with {max_rows} rows or fewer have been saved to {output_csv_folder}."
    )
    return output_csv_folder


class DistanceCalculator:
    def __init__(self, folder_path, output_folder_path, end_date):
        self.folder_path = folder_path
        self.output_folder_path = output_folder_path + "/03DISTANCE_CALCULATED"
        self.end_date = end_date

        if not os.path.exists(self.output_folder_path):
            os.makedirs(self.output_folder_path)

    # 날짜 파싱 함수
    def parse_date(self, date_str):
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except:
            return None

    # 영업 시기 문자열 생성
    def calculate_operating_period(self, df):
        operating_periods = []
        for _, row in df.iterrows():
            start_date = self.parse_date(row["인허가일자"])
            close_date = self.parse_date(row["폐업일자"])
            if not close_date:
                close_date = self.end_date
            operating_periods.append(f"{start_date.date()}~{close_date.date()}")
        return operating_periods

    # 거리 계산
    def calculate_average_distance(self, df):
        distances = []

        df = df.copy()
        df["start_date"] = df["영업 시기"].apply(
            lambda x: datetime.strptime(x.split("~")[0], "%Y-%m-%d")
        )
        df["end_date"] = df["영업 시기"].apply(
            lambda x: datetime.strptime(x.split("~")[1], "%Y-%m-%d")
        )

        # 좌표 데이터에서 NaN 또는 비정상 값 제거
        df = df.dropna(subset=["좌표정보x(epsg5174)", "좌표정보y(epsg5174)"])
        df = df[
            np.isfinite(df["좌표정보x(epsg5174)"])
            & np.isfinite(df["좌표정보y(epsg5174)"])
        ]

        for idx, row in df.iterrows():
            start_date, end_date = row["start_date"], row["end_date"]
            current_location = (row["좌표정보x(epsg5174)"], row["좌표정보y(epsg5174)"])

            # 월 단위 시점 샘플링
            dates = pd.date_range(start=start_date, end=end_date, freq="MS")

            min_distances = []

            for date in dates:
                # 현재 날짜에 영업 중인 다른 업장 필터링
                operating = df[
                    (df["사업장명"] != row["사업장명"])
                    & (df["start_date"] <= date)
                    & (df["end_date"] >= date)
                ]

                if len(operating) == 0:
                    continue

                # 좌표 배열 생성
                coords = (
                    operating[["좌표정보x(epsg5174)", "좌표정보y(epsg5174)"]]
                    .dropna()
                    .values
                )
                tree = KDTree(coords)
                dist, _ = tree.query([current_location], k=1)
                min_distances.append(dist[0])

            avg_distance = np.mean(min_distances) if min_distances else 0
            print(" 평균 최근접 거리:", avg_distance)
            distances.append(avg_distance)

        return df, distances

    # 개별 파일 처리 함수
    def process_file(self, file):
        try:
            print(f"🔄 처리 시작 add average distance -> {file}")
            file_path = os.path.join(self.folder_path, file)
            df = pd.read_csv(file_path, encoding="utf-8-sig", on_bad_lines="warn")

            # 필수 열 확인
            required_columns = {
                "인허가일자",
                "폐업일자",
                "좌표정보x(epsg5174)",
                "좌표정보y(epsg5174)",
                "사업장명",
            }

            if not required_columns.issubset(df.columns):
                print(f"⚠️ 필수 열 누락으로 스킵: {file}")
                missing_columns = required_columns - set(df.columns)
                print(f"누락된 열: {missing_columns}")
                return

            # 영업 시기 계산
            df["영업 시기"] = self.calculate_operating_period(df)

            # 평균 최근접 거리 계산
            df, distances = self.calculate_average_distance(df)
            df["동일업종 최근접거리의 평균"] = distances
            # 저장
            output_path = os.path.join(self.output_folder_path, file)
            df.to_csv(output_path, index=False)
            print(f"✅ 완료: {file}")

        except Exception as e:
            print(f"❌ 오류 발생: {file}")
            traceback.print_exc()

    # 모든 파일 처리 함수
    def process_all_files(self):
        print("=============================")
        print("=== add average distance ====")
        print("=============================")

        csv_files = [f for f in os.listdir(self.folder_path) if f.endswith(".csv")]

        with ThreadPoolExecutor() as executor:
            executor.map(self.process_file, csv_files)

        print("🎉 모든 파일 처리 완료!")


if __name__ == "__main__":
    INPUT_FOLDER = "C:/Users/bsh96/Documents/GitHub/spring_UDIK/DATA/LOCALDATA_ALL_CSV"
    OUTPUT_FOLDER = (
        "C:/Users/bsh96/Documents/GitHub/spring_UDIK/DATA/LOCALDATA_FILTERED_CSV"
    )

    # # 서울특별시 강남구 데이터만 필터링
    # output_csv_folder = filter_by_address(
    #     INPUT_FOLDER, OUTPUT_FOLDER, address_name="서울특별시 강남구"
    # )

    # # 개업연도가 2016년도이며 데이터만 필터링 (영업중/폐업 이외의 상태는 모두 제거)
    # output_csv_folder = filter_by_open_and_close(
    #     output_csv_folder, OUTPUT_FOLDER, open_year=2016
    # )

    # # 데이터가 50개 이하인 csv파일은 제외.
    # output_csv_folder = filter_small_csv_files(
    #     output_csv_folder,
    #     OUTPUT_FOLDER,
    #     max_rows=50,
    # )

    output_csv_folder = "C:/Users/bsh96/Documents/GitHub/spring_UDIK/DATA/LOCALDATA_FILTERED_CSV/01filtered"

    # 생존 column 추가
    output_csv_folder = add_survival_column(
        output_csv_folder,
        OUTPUT_FOLDER,
    )

    # 최근접 거리 계산
    calculator = DistanceCalculator(
        output_csv_folder, OUTPUT_FOLDER, end_date=datetime(2019, 12, 31)
    )
    calculator.process_all_files()

    print("All filtering and processing tasks completed successfully.")
