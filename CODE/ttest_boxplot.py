import os
import pandas as pd
from scipy.stats import ttest_ind
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import rc

# 경고 무시
import warnings

warnings.filterwarnings("ignore")

# ▶ 한글 폰트 설정
rc("font", family="Malgun Gothic")  # Windows의 경우 'Malgun Gothic' 사용
plt.rcParams["axes.unicode_minus"] = False  # 마이너스 기호 깨짐 방지

# ▶ 설정: CSV 파일이 들어있는 폴더 경로
csv_dir = "C:/Users/bsh96/Documents/GitHub/spring_UDIK/DATA/LOCALDATA_FILTERED_CSV/04adj_industry_added"
output_dir = "C:/Users/bsh96/Documents/GitHub/spring_UDIK/ANALYSIS_RESULT/00TTEST"
plot_dir = os.path.join(output_dir, "boxplots")
os.makedirs(plot_dir, exist_ok=True)

# ▶ 결과 저장용 리스트
t_test_results = []

# ▶ CSV 파일 반복
for file in os.listdir(csv_dir):
    if not file.endswith(".csv"):
        continue  # CSV 파일이 아닌 경우 건너뛰기

    file_path = os.path.join(csv_dir, file)
    df = pd.read_csv(file_path)
    # 업종명
    industry_name = file.replace(".csv", "").split("_")[-1]

    # 컬럼 확인
    alive = df[df["생존"] == True]["adj_industry_count"].dropna()
    dead = df[df["생존"] == False]["adj_industry_count"].dropna()

    if not (len(alive) > 1 and len(dead) > 1):
        continue  # 생존/폐업 샘플 수가 2개 이상이어야 함

    # ▶ T-test
    t_stat, p_val = ttest_ind(alive, dead, equal_var=False)

    # ▶ 결과 저장
    significance = ""
    if p_val < 0.001:
        significance = "***"
    elif p_val < 0.01:
        significance = "**"
    elif p_val < 0.05:
        significance = "*"

    t_test_results.append(
        {
            "업종명": industry_name,
            "생존_업장 수": len(alive),
            "폐업_업장 수": len(dead),
            "t값": round(t_stat, 3),
            "p값": round(p_val, 5),
            "유의성": significance,
        }
    )

    # # ▶ 박스플롯 저장
    # plt.figure(figsize=(6, 4))
    # sns.boxplot(data=df, x="생존", y="동일업종 최근접거리의 평균")
    # plt.title(f"Boxplot - {industry_name}")
    # plt.tight_layout()
    # plt.savefig(os.path.join(plot_dir, f"{industry_name}_boxplot.png"))
    # plt.close()

# ▶ 결과 DataFrame 저장
result_df = pd.DataFrame(t_test_results)
result_df.to_csv(
    os.path.join(output_dir, "t_test_summary_adj_count.csv"),
    index=False,
    encoding="utf-8-sig",
)

print("✅ 모든 분석 완료! 결과 파일 및 박스플롯 이미지가 저장되었습니다.")
