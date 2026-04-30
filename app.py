import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import time

st.set_page_config(page_title="老周法人真愛股", layout="wide")
st.title("🚀 老周法人真愛股分析 App")

def get_data(date_str):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={date_str}&selectType=ALL"
    try:
        for _ in range(2):
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                if data.get('stat') == 'OK':
                    return pd.DataFrame(data['data'], columns=data['fields'])
            time.sleep(1)
    except:
        return None
    return None

st.sidebar.header("分析設定")
days_to_check = st.sidebar.slider("要比對幾天有開盤的資料？", 2, 10, 3)

if st.button("🔍 開始自動分析"):
    with st.spinner("正在努力連線證交所..."):
        all_dfs = []
        current_date = datetime.now()
        attempts = 0
        while len(all_dfs) < days_to_check and attempts < 15:
            date_str = current_date.strftime("%Y%m%d")
            df = get_data(date_str)
            if df is not None:
                all_dfs.append(df)
                st.write(f"✅ 成功獲取 {date_str} 數據")
            current_date -= timedelta(days=1)
            attempts += 1
            
        if len(all_dfs) >= 2:
            base_df = all_dfs[0]
            for other_df in all_dfs[1:]:
                base_df = base_df[base_df['證券代號'].isin(other_df['證券代號'])]
            st.success(f"分析完成！共比對最近 {len(all_dfs)} 個交易日。")
            st.dataframe(base_df, use_container_width=True)
        else:
            st.warning("證交所伺服器忙碌中，請等一下再按一次分析！")
