import json
import requests

url = requests.get("http://ecos.bok.or.kr/api/StatisticSearch/0GK5VA3EB3WSUUSD9C2Q/json/kr/1/100000/403Y004/M/198801/202203")
text = url.text

data = json.loads(text)

with open("/home/big/predict_gas/raw_data/income_amount/income_amount1.json", "w") as json_file: json.dump(data, json_file, ensure_ascii=False)


url = requests.get("http://ecos.bok.or.kr/api/StatisticSearch/0GK5VA3EB3WSUUSD9C2Q/json/kr/1/100000/403Y004/M/202204/202206")
text = url.text

data = json.loads(text)

with open("/home/big/predict_gas/raw_data/income_amount/income_amount2.json", "w") as json_file: json.dump(data, json_file, ensure_ascii=False)
