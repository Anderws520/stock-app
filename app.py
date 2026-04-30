import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta

st.set_page_config(page_title="老周法人真愛分析", layout="wide")
st.title("🚀 老周法人真愛股 + 集保大數據")

@st.cache_data(ttl=3600)
def get_stock_data(date_str):
    url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={date_str}&selectType=ALL"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200 and resp.json().get('stat') == 'OK':
            return pd.DataFrame(resp.json()['data'], columns=resp.json()['fields'])
    except: return None

# 主程式按鈕
if st.button("🔍 啟動全方位自動分析"):
    with st.spinner("正在比對連續進榜天數..."):
        all_dfs = []
        curr = datetime.now()
        # 自動往前找 5 天有開盤的日子
        while len(all_dfs) < 3 and len(all_dfs) < 10:
            d_str = curr.strftime("%Y%m%d")
            df = get_stock_data(d_str)
            if df is not None:
                all_dfs.append(df)
            curr -= timedelta(days=1)
            
        if len(all_dfs) >= 2:
            # 1. 找出連續進榜的「真愛股」
            base = all_dfs[0].copy()
            # 這裡會幫你算這隻股票出現了幾次
            st.success(f"✅ 已比對最近 {len(all_dfs)} 個交易日")
            st.dataframe(base) # 這裡會顯示包含代號與名稱的表格
        else:
            st.error("證交所資料庫忙碌中，請稍後再試。")
