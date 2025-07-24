import requests
from seatable_api import Base

# ============================ 配置参数 ============================
# 1. 数据接口
WATER_API_URL = "http://58.247.45.108:8020//RegionalWaterAnalysis/getWA_Stcd8"


# 2. SeaTable配置
SEATABLE_SERVER_URL = "https://mis.cityfun.com.cn/"
SEATABLE_API_TOKEN = "c22b6db1054d5d40e471f62cce3ba94ad0856aa0"
SEATABLE_TABLE_NAME = "实时水情"

# 3. pushplus配置
PUSHPLUS_TOKEN = "36ca1d93b58a4ae39a7f2e81908c92fa"
PUSHPLUS_URL = "https://www.pushplus.plus/send"
PUSHPLUS_TOPIC = "wx_web_spider"
# ============================ 配置参数结束 ============================

def get_water_data():
    """从指定API获取当前日期的水情数据。"""
    today = datetime.date.today().strftime("%Y-%m-%d")
    headers = {
        'DNT': '1',
        'Proxy-Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
    }
    data = f'stime={today}'
    print(f"Fetching water data for date: {today} from {WATER_API_URL}")
    try:
        response = requests.post(WATER_API_URL, headers=headers, data=data, timeout=30)
        response.raise_for_status() # 检查HTTP请求是否成功 (2xx状态码)
        json_data = response.json()
        print(f"Received data (total: {json_data.get('total')}, success: {json_data.get('success')})")
        return json_data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching water data: {e}")
        return None
    except ValueError as e: # 捕获 JSON 解码错误
        print(f"Error decoding JSON response: {e}, Response content: {response.text}")
        return None

def save_to_seatable(records):
    # 使用seatable-api库进行写入
    base = Base(SEATABLE_API_TOKEN, SEATABLE_SERVER_URL)
    base.auth()
    for rec in records:
        row = {
            "stcd": rec.get("stcd", ""),
            "stnm": rec.get("stnm", ""),
            "lttd": rec.get("lttd", ""),
            "lgtd": rec.get("lgtd", ""),
            "wrz": rec.get("wrz", ""),
            "z": rec.get("z", "")
            "grz": rec.get("grz", "")
        }
        # 过滤掉无效字段
        row = {k: v for k, v in row.items() if v != "" and v is not None}
        base.append_row(SEATABLE_TABLE_NAME, row)

def push_notification(records):
    # 构建消息内容
    payload = {
        "token": PUSHPLUS_TOKEN,
        "topic": PUSHPLUS_TOPIC,
        "title": "太湖流域片水文信息服务代表站水位实时水情",
        "content": "执行成功",
        "template": "txt"
    }
    resp = requests.post(PUSHPLUS_URL, json=payload)
    resp.raise_for_status()
    return resp.json()

def main():
    # Step 1: 获取数据
    data = get_water_data()
    if not data or data.get("code") != 200 or not data.get("data", {}).get("IsSuccess"):
        print("获取水情数据失败", data)
        return
    records = data["data"]["data"]
    # Step 2: 保存到 SeaTable
    try:
        save_to_seatable(records)
    except Exception as e:
        print(f"保存到SeaTable失败: {e}")
    # Step 3: 推送消息通知
    try:
        push_result = push_notification(records)
        print("推送结果:", push_result)
    except Exception as e:
        print(f"Pushplus推送失败: {e}")

if __name__ == "__main__":
    main()
