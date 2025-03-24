import os
import pandas as pd

# MERGED_CSV 폴더 경로와 SEOUL 폴더 경로 설정
merged_csv_folder = "C:/Users/bsh96/Desktop/2025-1/춘계학술대회/DATA/MERGED_CSV"
seoul_folder = "C:/Users/bsh96/Desktop/2025-1/춘계학술대회/DATA/FILTERED_CSV/SEOUL"

# SEOUL 폴더가 없으면 생성
if not os.path.exists(seoul_folder):
    os.makedirs(seoul_folder)

# MERGED_CSV 폴더 내의 모든 CSV 파일 처리
for file_name in os.listdir(merged_csv_folder):
    if file_name.endswith(".csv"):
        file_path = os.path.join(merged_csv_folder, file_name)

        # CSV 파일 읽기
        df = pd.read_csv(file_path)

        # "소재지전체주소" 열에서 "서울특별시"가 포함된 데이터 필터링
        if "소재지전체주소" in df.columns:
            filtered_df = df[df["소재지전체주소"].str.contains("서울특별시", na=False)]

            # 필터링된 데이터를 SEOUL 폴더에 저장
            output_path = os.path.join(seoul_folder, file_name)
            filtered_df.to_csv(output_path, index=False, encoding="utf-8-sig")
