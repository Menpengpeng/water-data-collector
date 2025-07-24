import requests
import datetime
from seatable_api import Base, ColumnTypes

# --- Configuration ---
SEATABLE_SERVER_URL = "https://mis.cityfun.com.cn/"
SEATABLE_API_TOKEN = "c22b6db1054d5d40e471f62cce3ba94ad0856aa0"
SEATABLE_TABLE_NAME = "实时水情"

PUSHPLUS_TOKEN = "36ca1d93b58a4ae39a7f2e81908c92fa"
PUSHPLUS_URL = "https://www.pushplus.plus/send"
PUSHPLUS_TOPIC = "wx_web_spider"

WATER_API_URL = "http://58.247.45.108:8020//RegionalWaterAnalysis/getWA_Stcd8"

# --- Functions ---

def get_water_data():
    """Fetches water data from the specified API for the current date."""
    today = datetime.date.today().strftime("%Y-%m-%d")
    headers = {
        'DNT': '1',
        'Proxy-Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
    }
    data = f'stime={today}'
    print(f"Fetching water data for date: {today}")
    try:
        response = requests.post(WATER_API_URL, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching water data: {e}")
        return None

def save_data_to_seatable(data):
    """Saves the provided data to the SeaTable."""
    base = Base(SEATABLE_API_TOKEN, SEATABLE_SERVER_URL)
    try:
        base.auth()
        print("Successfully connected to SeaTable.")

        # Ensure the table and columns exist, or create them
        metadata = base.get_metadata()
        table_exists = False
        for table in metadata.get('tables', []):
            if table.get('name') == SEATABLE_TABLE_NAME:
                table_exists = True
                break

        if not table_exists:
            print(f"Table '{SEATABLE_TABLE_NAME}' not found. Creating it...")
            base.create_table(SEATABLE_TABLE_NAME, lang='zh-CN') # lang='zh-CN' for Chinese template
            # Define columns if the table is new. Adjust types as needed.
            columns_to_create = [
                {"name": "站点代码", "type": ColumnTypes.TEXT},
                {"name": "站名", "type": ColumnTypes.TEXT},
                {"name": "纬度", "type": ColumnTypes.NUMBER},
                {"name": "经度", "type": ColumnTypes.NUMBER},
                {"name": "警戒水位", "type": ColumnTypes.NUMBER},
                {"name": "水位", "type": ColumnTypes.NUMBER},
                {"name": "保证水位", "type": ColumnTypes.NUMBER},
            ]
            for col_def in columns_to_create:
                base.insert_column(SEATABLE_TABLE_NAME, col_def)
            print("Table and columns created.")
        else:
            print(f"Table '{SEATABLE_TABLE_NAME}' already exists. Checking columns...")
            # You might want to add logic here to check for missing columns and create them

        # Clear existing data in the table if you want to always have fresh data
        # rows = base.list_rows(SEATABLE_TABLE_NAME)
        # if rows:
        #     row_ids_to_delete = [row['_id'] for row in rows]
        #     base.batch_delete_rows(SEATABLE_TABLE_NAME, row_ids_to_delete)
        #     print(f"Cleared {len(row_ids_to_delete)} existing rows in '{SEATABLE_TABLE_NAME}'.")

        rows_to_add = []
        for item in data.get('data', []):
            row = {
                "站点代码": item.get('stcd'),
                "站名": item.get('stnm'),
                "纬度": float(item.get('lttd')) if item.get('lttd') else None,
                "经度": float(item.get('lgtd')) if item.get('lgtd') else None,
                "警戒水位": float(item.get('wrz')) if item.get('wrz') else None,
                "水位": float(item.get('z')) if item.get('z') else None,
                "保证水位": float(item.get('grz')) if item.get('grz') else None,
            }
            rows_to_add.append(row)

        if rows_to_add:
            base.batch_append_rows(SEATABLE_TABLE_NAME, rows_to_add)
            print(f"Successfully added {len(rows_to_add)} rows to SeaTable.")
        else:
            print("No data to add to SeaTable.")

    except Exception as e:
        print(f"Error saving data to SeaTable: {e}")

def send_push_notification(title, content):
    """Sends a push notification using Pushplus."""
    payload = {
        "token": PUSHPLUS_TOKEN,
        "title": title,
        "content": content,
        "topic": PUSHPLUS_TOPIC
    }
    try:
        response = requests.post(PUSHPLUS_URL, json=payload)
        response.raise_for_status()
        result = response.json()
        if result.get('code') == 200:
            print("Push notification sent successfully.")
        else:
            print(f"Failed to send push notification: {result.get('msg')}")
    except requests.exceptions.RequestException as e:
        print(f"Error sending push notification: {e}")

# --- Main Script Execution ---

if __name__ == "__main__":
    print("Starting water data processing...")

    # 1. Fetch data
    water_data = get_water_data()

    if water_data and water_data.get('success') == 'true' and water_data.get('data'):
        # 2. Save data to SeaTable
        save_data_to_seatable(water_data)

        # 3. Prepare and send push notification
        total_records = water_data.get('total', '0')
        notification_title = "实时水情数据更新"
        notification_content = f"已成功获取并更新 {total_records} 条实时水情数据到 SeaTable。\n\n部分数据预览：\n"
        
        # Add a few lines of data preview to the notification
        for i, item in enumerate(water_data['data'][:3]): # show first 3 items
            notification_content += f"- 站名: {item.get('stnm')}, 水位: {item.get('z')}m\n"
        
        send_push_notification(notification_title, notification_content)
    elif water_data:
        print("No water data found or API returned an error.")
        send_push_notification("实时水情数据获取失败", "未能获取到实时水情数据，请检查API接口。")
    else:
        print("Failed to fetch water data.")
        send_push_notification("实时水情数据获取失败", "获取实时水情数据时发生网络错误或API无响应。")

    print("Script finished.")
