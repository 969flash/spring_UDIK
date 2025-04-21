import os
import pandas as pd
import statsmodels.api as sm
from sklearn.metrics import accuracy_score

# ▶ 설정: CSV 파일이 들어있는 폴더 경로
input_folder = "C:/Users/bsh96/Documents/GitHub/spring_UDIK/DATA/LOCALDATA_FILTERED_CSV/04adj_industry_added"
output_folder = (
    "C:/Users/bsh96/Documents/GitHub/spring_UDIK/ANALYSIS_RESULT/01LOGISTIC_REGRESSION"
)
result_csv_name = "logistic_summary.csv"
result_list = []

for file_name in os.listdir(input_folder):
    if not file_name.endswith(".csv"):
        continue

    file_path = os.path.join(input_folder, file_name)

    # 업종명
    industry_name = file_name.replace(".csv", "").split("_")[-1]
    df = pd.read_csv(file_path, encoding="utf-8-sig")

    # 필수 컬럼 확인
    required_columns = ["생존", "동일업종 최근접거리의 평균", "adj_industry_count"]
    if not all(col in df.columns for col in required_columns):
        continue

    # 결측 제거
    df = df[required_columns].dropna()

    if df["생존"].nunique() < 2 or len(df) < 10:
        continue  # 생존 값이 전부 동일하거나 너무 적으면 skip

    # X, y 설정
    X = df[["동일업종 최근접거리의 평균", "adj_industry_count"]]
    X = sm.add_constant(X)  # 상수항 추가
    y = df["생존"].astype(int)

    # 모델 피팅
    model = sm.Logit(y, X).fit(disp=0)

    # 예측 및 정확도 계산
    pred_probs = model.predict(X)
    y_pred = (pred_probs >= 0.5).astype(int)
    accuracy = accuracy_score(y, y_pred)

    # McFadden R² 계산
    mcfadden_r2 = 1 - (model.llf / model.llnull)

    # p-value 및 유의성 계산
    p_values = model.pvalues
    significance_distance = ""
    significance_adj_count = ""

    if p_values["동일업종 최근접거리의 평균"] < 0.001:
        significance_distance = "***"
    elif p_values["동일업종 최근접거리의 평균"] < 0.01:
        significance_distance = "**"
    elif p_values["동일업종 최근접거리의 평균"] < 0.05:
        significance_distance = "*"

    if p_values["adj_industry_count"] < 0.001:
        significance_adj_count = "***"
    elif p_values["adj_industry_count"] < 0.01:
        significance_adj_count = "**"
    elif p_values["adj_industry_count"] < 0.05:
        significance_adj_count = "*"

    # 결과 저장
    result_list.append(
        {
            "업종명": industry_name,
            "coef(거리)": round(model.params["동일업종 최근접거리의 평균"], 4),
            "p값(거리)": round(p_values["동일업종 최근접거리의 평균"], 5),
            "P값 유의성(거리)": significance_distance,
            "coef(인접 업종 수)": round(model.params["adj_industry_count"], 4),
            "p값(인접 업종 수)": round(p_values["adj_industry_count"], 5),
            "P값 유의성(인접 업종 수)": significance_adj_count,
            "정확도": round(accuracy, 3),
            "McFadden R²": round(mcfadden_r2, 4),
            "샘플수": len(df),
        }
    )

# 결과 저장
result_df = pd.DataFrame(result_list)
result_path = os.path.join(output_folder, result_csv_name)
result_df.to_csv(result_path, index=False, encoding="utf-8-sig")

print("✅ 로지스틱 회귀 분석 완료! 결과 저장:", result_path)
