import os
import pandas as pd
from collections import defaultdict

# 데이터 디렉토리 경로 (절대경로로 수정)
data_dir = "C:/Users/bsh96/Desktop/2025-1/춘계학술대회/DATA/LOCALDATA_ALL_CSV"

# 중분류 기준으로 파일 그룹화
file_groups = defaultdict(list)

# 파일 이름을 기준으로 그룹화
for file_name in os.listdir(data_dir):
    if file_name.endswith(".csv"):
        # 파일 이름에서 중분류 키 추출 (fulldata_01_01)
        parts = file_name.split("_")
        if len(parts) >= 4:
            group_key = "_".join(parts[:3])  # fulldata_01_01
            file_groups[group_key].append(file_name)

# 중분류별로 파일 병합
for group_key, files in file_groups.items():
    merged_data = pd.DataFrame()

    for file_name in files:
        file_path = os.path.join(data_dir, file_name)
        try:
            # CSV 파일 읽기 (on_bad_lines 추가)
            data = pd.read_csv(file_path, encoding="cp949", on_bad_lines="skip")
            # 데이터 병합
            merged_data = pd.concat([merged_data, data], ignore_index=True)
        except (UnicodeDecodeError, pd.errors.ParserError) as e:
            print(f"Error in file {file_name}: {e}")
            continue

    # 병합된 파일 저장
    output_file = os.path.join(data_dir, f"{group_key}_merged.csv")
    merged_data.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"Merged {len(files)} files into {output_file}")
