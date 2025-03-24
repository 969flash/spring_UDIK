#### QGIS Python Script ####

import os
from qgis.core import QgsVectorLayer, QgsProject

# CSV 파일들이 있는 폴더 경로
csv_folder = "C:/Users/bsh96/Desktop/2025-1/춘계학술대회/DATA/MERGED_CSV"

# 좌표 컬럼명 (예: X: 'longitude', Y: 'latitude')
x_field = "좌표정보x(epsg5174)"
y_field = "좌표정보y(epsg5174)"

# 폴더 내 모든 CSV 파일에 대해 반복
for filename in os.listdir(csv_folder):
    if filename.endswith(".csv"):
        path = os.path.join(csv_folder, filename)
        layer_name = os.path.splitext(filename)[0]

        uri = f"file:///{path}?delimiter=,&xField={x_field}&yField={y_field}&crs=EPSG:5174"
        layer = QgsVectorLayer(uri, layer_name, "delimitedtext")

        if layer.isValid():
            QgsProject.instance().addMapLayer(layer)
        else:
            print(f"Layer failed to load: {filename}")
