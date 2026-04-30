import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import time
import random

st.set_page_config(page_title="老周法人真愛分析", layout="wide")
st.title("🚀 老周法人真愛股 + 集保大數據")

# 這裡把拉霸找回來，方便老周手動調整
st.sidebar.header("分析設定")
days_to_check = st.sidebar.slider("比對過去幾天？", 2, 5, 2)

@st.cache_data(ttl=600)
def get_stock_data(date_str):
    # 使用完整的瀏覽器資訊偽裝
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={date_str}&selectType=ALL"
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('stat') == 'OK':
                return pd.DataFrame(data['data'], columns=data['fields'])
    except Exception as e:
        return None
    return None

if st.button("🔍 啟動全方位自動分析"):
    all_dfs = []
    curr = datetime.now()
    attempts = 0
    
    # 加入進度條，讓你隨時知道抓到哪了
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    with st.spinner("正在執行『裝死策略』連線中..."):
        while len(all_dfs) < days_to_check and attempts < 15:
            d_str = curr.strftime("%Y%m%d")
            status_text.text(f"正在請求 {d_str} 的資料...")
            
            df = get_stock_data(d_str)
            if df is not None:
                all_dfs.append(df)
                st.write(f"✅ {d_str} 抓取成功！")
                # --- 關鍵裝死點：抓到後休息久一點，模擬真人在看表格 ---
                wait_time = random.uniform(3, 5) 
                time.sleep(wait_time) 
            else:
                # 如果抓不到，可能是假日或被擋，稍微休息一下繼續往前找
                time.sleep(1)
            
            curr -= timedelta(days=1)
            attempts += 1
            progress_bar.progress(min(len(all_dfs) / days_to_check, 1.0))
            
        if len(all_dfs) >= 2:
            # 進行連連看分析
            base = all_dfs[0].copy()
            for other in all_dfs[1:]:
                base = base[base['證券代號'].isin(other['證券代號'])]
            
            st.success(f"老周，分析好了！排除假日後，連續 {len(all_dfs)} 天進榜的名單如下：")
            st.dataframe(base, use_container_width=True)
        else:
            st.warning("⚠️ 證交所門禁森嚴中，請過 5 分鐘再按一次按鈕，或將『比對天數』調低。")
