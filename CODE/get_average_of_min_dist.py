# -*- coding:utf-8 -*-
import os
import pandas as pd
import numpy as np
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from scipy.spatial import KDTree
import traceback

# 폴더 경로 설정
folder_path = (
    "C:/Users/bsh96/Desktop/2025-1/춘계학술대회/DATA/FILTERED_CSV/SEOUL/STATUS_FILTERED"
)
output_folder_path = (
    "C:/Users/bsh96/Desktop/2025-1/춘계학술대회/DATA/FILTERED_CSV/SEOUL/DATA_FINAL"
)
if not os.path.exists(output_folder_path):
    os.makedirs(output_folder_path)

# 기준일 (2019년 12월 31일)
end_date = datetime(2019, 12, 31)


# 날짜 파싱 함수
def parse_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except:
        return None


# 영업 시기 문자열 생성
def calculate_operating_period(df):
    operating_periods = []
    for _, row in df.iterrows():
        start_date = parse_date(row["인허가일자"])
        close_date = parse_date(row["폐업일자"])
        if not close_date:
            close_date = end_date
        operating_periods.append(f"{start_date.date()}~{close_date.date()}")
    return operating_periods


# 거리 계산
def calculate_average_distance(df):
    distances = []

    df = df.copy()
    df["start_date"] = df["영업 시기"].apply(
        lambda x: datetime.strptime(x.split("~")[0], "%Y-%m-%d")
    )
    df["end_date"] = df["영업 시기"].apply(
        lambda x: datetime.strptime(x.split("~")[1], "%Y-%m-%d")
    )

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

    return distances


# 개별 파일 처리 함수
def process_file(file):
    try:
        print(f"🔄 처리 시작: {file}")
        file_path = os.path.join(folder_path, file)
        df = pd.read_csv(file_path)

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
        df["영업 시기"] = calculate_operating_period(df)

        # 평균 최근접 거리 계산
        df["동일업종 최근접거리의 평균"] = calculate_average_distance(df)

        # 저장
        output_path = os.path.join(output_folder_path, file)
        df.to_csv(output_path, index=False)
        print(f"✅ 완료: {file}")

    except Exception as e:
        print(f"❌ 오류 발생: {file}")
        traceback.print_exc()


# 모든 CSV 파일 불러오기
csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]

# 병렬 처리 실행
if __name__ == "__main__":
    with ThreadPoolExecutor() as executor:
        executor.map(process_file, csv_files)

    print("🎉 모든 파일 처리 완료!")
