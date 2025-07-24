import requests
import datetime
import os
from seatable_api import Base
from seatable_api.constants import ColumnTypes # 修正 ColumnTypes 的导入

# ============================ 配置参数 ============================
# 建议使用GitHub Secrets来存储敏感信息，例如在GitHub仓库设置中添加：
# SEATABLE_SERVER_URL: https://mis.cityfun.com.cn/
# SEATABLE_API_TOKEN: <您的 SeaTable API Token>
# PUSHPLUS_TOKEN: <您的 Pushplus Token>
# PUSHPLUS_TOPIC: <您的 Pushplus 群组编码> (例如 wx_web_spider)

# 1. 数据接口
WATER_API_URL = "http://58.247.45.108:8020//RegionalWaterAnalysis/getWA_Stcd8"
# 接口的 headers 和 data-urlencode 'stime' 会在函数中动态生成

# 2. SeaTable配置
SEATABLE_SERVER_URL = os.environ.get("SEATABLE_SERVER_URL", "https://mis.cityfun.com.cn/")
SEATABLE_API_TOKEN = os.environ.get("SEATABLE_API_TOKEN") # 从环境变量获取
SEATABLE_TABLE_NAME = "实时水情"

# 3. pushplus配置
PUSHPLUS_TOKEN = os.environ.get("PUSHPLUS_TOKEN") # 从环境变量获取
PUSHPLUS_URL = os.environ.get("PUSHPLUS_URL", "https://www.pushplus.plus/send")
PUSHPLUS_TOPIC = os.environ.get("PUSHPLUS_TOPIC", "wx_web_spider")
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

def ensure_seatable_structure(base_instance):
    """
    检查 SeaTable 表和列是否存在。如果不存在则创建。
    """
    table_name = SEATABLE_TABLE_NAME
    # 定义water_monitor.py所需的列及其类型，与您的截图一致
    expected_columns = {
        "stcd": ColumnTypes.TEXT,
        "stnm": ColumnTypes.TEXT,
        "lttd": ColumnTypes.NUMBER,
        "lgtd": ColumnTypes.NUMBER,
        "wrz": ColumnTypes.NUMBER,
        "z": ColumnTypes.NUMBER,
        "grz": ColumnTypes.NUMBER,
    }

    try:
        metadata = base_instance.get_metadata()
        tables = metadata.get('tables', [])
        table_exists = False
        for table in tables:
            if table.get('name') == table_name:
                table_exists = True
                break

        if not table_exists:
            print(f"Table '{table_name}' not found. Creating it...")
            base_instance.create_table(table_name)
            print(f"Table '{table_name}' created.")

        # 检查并创建缺失的列
        existing_columns = base_instance.list_columns(table_name)
        existing_column_names = {col['name'] for col in existing_columns}

        for col_name, col_type in expected_columns.items():
            if col_name not in existing_column_names:
                print(f"Column '{col_name}' not found in table '{table_name}'. Creating it as {col_type}...")
                base_instance.insert_column(table_name, {"name": col_name, "type": col_type})
                print(f"Column '{col_name}' created.")
            # else: 
            # 也可以在这里添加逻辑来检查现有列的类型是否正确，但通常不建议自动更改列类型，因为可能导致数据丢失

    except Exception as e:
        print(f"Error ensuring SeaTable structure: {e}")
        raise # 重新抛出异常，阻止后续的数据保存操作

def save_data_to_seatable(data):
    """将获取到的数据保存到 SeaTable。"""
    if not SEATABLE_API_TOKEN:
        print("Error: SEATABLE_API_TOKEN is not set. Cannot save to SeaTable.")
        # 发送通知提醒API Token未设置
        send_push_notification("SeaTable保存失败", "SEATABLE_API_TOKEN 未设置，无法保存数据。")
        return

    base = Base(SEATABLE_API_TOKEN, SEATABLE_SERVER_URL)
    try:
        base.auth()
        print("Successfully authenticated with SeaTable.")

        # 确保表和列结构正确
        ensure_seatable_structure(base)

        # 清除现有数据 (如果您希望每次运行都刷新数据而不是追加，取消注释以下代码块)
        # rows = base.list_rows(SEATABLE_TABLE_NAME)
        # if rows:
        #     row_ids_to_delete = [row['_id'] for row in rows]
        #     print(f"Clearing {len(row_ids_to_delete)} existing rows from '{SEATABLE_TABLE_NAME}'...")
        #     base.batch_delete_rows(SEATABLE_TABLE_NAME, row_ids_to_delete)
        #     print("Existing rows cleared.")

        rows_to_add = []
        for item in data.get('data', []):
            row = {
                "stcd": str(item.get('stcd', '')),
                "stnm": str(item.get('stnm', '')),
                "lttd": float(item.get('lttd')) if item.get('lttd') is not None else None,
                "lgtd": float(item.get('lgtd')) if item.get('lgtd') is not None else None,
                "wrz": float(item.get('wrz')) if item.get('wrz') is not None else None,
                "z": float(item.get('z')) if item.get('z') is not None else None,
                "grz": float(item.get('grz')) if item.get('grz') is not None else None,
            }
            # 过滤掉None值，避免写入空字符串或导致SeaTable问题
            row_filtered = {k: v for k, v in row.items() if v is not None}
            rows_to_add.append(row_filtered)

        if rows_to_add:
            print(f"Adding {len(rows_to_add)} new rows to SeaTable...")
            base.batch_append_rows(SEATABLE_TABLE_NAME, rows_to_add)
            print(f"Successfully added {len(rows_to_add)} rows to SeaTable.")
        else:
            print("No valid records to add to SeaTable.")

    except Exception as e:
        print(f"Error saving data to SeaTable: {e}")
        send_push_notification("SeaTable保存失败", f"保存水情数据到SeaTable失败: {e}") # 发生错误时发送通知

def send_push_notification(title, content):
    """使用 Pushplus 发送消息通知。"""
    if not PUSHPLUS_TOKEN:
        print("Error: PUSHPLUS_TOKEN is not set. Cannot send push notification.")
        return

    payload = {
        "token": PUSHPLUS_TOKEN,
        "title": title,
        "content": content,
        "topic": PUSHPLUS_TOPIC,
        "template": "txt" # 显式指定文本模板
    }
    print(f"Attempting to send push notification to topic: {PUSHPLUS_TOPIC} with title: '{title}'")
    try:
        resp = requests.post(PUSHPLUS_URL, json=payload, timeout=10) # 增加超时时间
        resp.raise_for_status() # 检查HTTP请求是否成功 (2xx状态码)
        push_result = resp.json()
        if push_result.get('code') == 200:
            print("Push notification sent successfully.")
        else:
            print(f"Failed to send push notification: {push_result.get('msg', 'Unknown error')}")
        return push_result
    except requests.exceptions.RequestException as e:
        print(f"Pushplus notification failed: {e}")
        return None
    except ValueError as e:
        print(f"Error decoding Pushplus JSON response: {e}, Response content: {resp.text}")
        return None

# --- Main Script Execution ---
if __name__ == "__main__":
    print(f"Script started at: {datetime.datetime.now()} JST") # 明确时区

    # 1. Fetch data
    water_data = get_water_data()

    if not water_data:
        print("未能获取到水情数据，脚本中止。")
        send_push_notification("水情数据获取失败", "未能从API获取到水情数据，请检查网络或API状态。")
        exit(1) # 非正常退出

    # 检查API返回的业务逻辑成功标志
    # 根据您提供的API返回数据结构，成功时 'success' 为 "true"，并且 'data' 存在
    if water_data.get('success') != 'true' or not water_data.get('data'):
        error_msg = water_data.get('message', 'API返回的数据不符合预期或没有记录。')
        print(f"获取水情数据失败: Success={water_data.get('success')}, Has Data={bool(water_data.get('data'))}, Message={error_msg}")
        send_push_notification("水情数据获取失败", f"API返回错误或无数据: {error_msg}")
        exit(1) # 非正常退出

    records = water_data['data']
    print(f"成功获取 {len(records)} 条水情记录。")

    # 2. Save data to SeaTable
    try:
        # save_data_to_seatable 内部会处理数据的提取和结构检查
        save_data_to_seatable(water_data)
    except Exception as e:
        print(f"主流程中保存到SeaTable失败: {e}")
        # save_data_to_seatable 内部已发送错误通知，这里可以不做额外处理

    # 3. Prepare and send push notification
    notification_title = "实时水情数据更新"
    total_records_str = water_data.get('total', str(len(records))) # 优先使用API返回的total字段
    notification_content = f"已成功获取并处理 {total_records_str} 条实时水情数据。\n\n部分数据预览：\n"

    # 限制通知内容长度，避免超出Pushplus限制，只取前N条
    num_preview_records = min(len(records), 5) # 最多显示5条
    for i in range(num_preview_records):
        item = records[i]
        line = f"- {item.get('stnm','未知站名')}({item.get('stcd','未知代码')}):"
        if item.get('z') is not None:
            line += f" 水位:{item.get('z')}m"
        if item.get('wrz') is not None:
            line += f" 警戒:{item.get('wrz')}m"
        notification_content += line + "\n"
    if len(records) > num_preview_records:
        notification_content += f"... 共有 {len(records)} 条记录\n"
    
    # 确保内容不会过长，通常Pushplus限制为5000字
    if len(notification_content) > 4000:
        notification_content = notification_content[:4000] + "\n...(内容过长，已截断)"


    try:
        push_result = send_push_notification(notification_title, notification_content)
        # send_push_notification 内部已打印推送结果
    except Exception as e:
        print(f"主流程中Pushplus推送失败: {e}")

    print(f"Script finished at: {datetime.datetime.now()} JST")
