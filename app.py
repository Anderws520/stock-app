import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import time
import random

st.set_page_config(page_title="老周法人真愛分析", layout="wide")
st.title("🚀 老周法人真愛股 + 集保大數據")

# --- 側邊欄設定 (拉霸在這裡！) ---
st.sidebar.header("分析設定")
days_to_check = st.sidebar.slider("比對過去幾天？", 2, 5, 2)
st.sidebar.write("💡 提示：若抓不到資料，請將天數調低為 2。")

def get_stock_data(date_str):
    # 增加隨機參數避免被證交所記憶
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={date_str}&selectType=ALL&random={random.random()}"
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('stat') == 'OK':
                return pd.DataFrame(data['data'], columns=data['fields'])
    except: return None
    return None

if st.button("🔍 啟動全方位自動分析"):
    all_dfs = []
    # 從昨天開始抓比較穩，因為今天(4/30)的資料雲端主機可能還連不上
    curr = datetime.now() - timedelta(days=1) 
    attempts = 0
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    with st.spinner("正在連線證交所 (執行裝死策略)..."):
        # 最多往前找 8 天，直到湊齊拉霸設定的天數
        while len(all_dfs) < days_to_check and attempts < 8:
            d_str = curr.strftime("%Y%m%d")
            status_text.text(f"正在請求 {d_str} 的資料...")
            
            df = get_stock_data(d_str)
            if df is not None:
                all_dfs.append(df)
                st.write(f"✅ {d_str} 抓取成功！")
                time.sleep(random.uniform(3, 5)) # 抓到後裝死久一點
            else:
                st.write(f"❌ {d_str} 暫無資料或連線被拒")
                time.sleep(1)
            
            curr -= timedelta(days=1)
            attempts += 1
            progress_bar.progress(min(attempts / 8, 1.0))
            
        if len(all_dfs) >= 2:
            base = all_dfs[0].copy()
            for other in all_dfs[1:]:
                base = base[base['證券代號'].isin(other['證券代號'])]
            
            st.success(f"老周，分析好了！共比對最近 {len(all_dfs)} 個交易日。")
            st.dataframe(base, use_container_width=True)
        else:
            st.error("⚠️ 門神擋路中！目前雲端 IP 被證交所暫時限制，請 10 分鐘後再試。")
