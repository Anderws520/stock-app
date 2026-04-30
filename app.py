import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import time

st.set_page_config(page_title="老周法人真愛分析", layout="wide")
st.title("🚀 老周法人真愛股 + 集保大數據")

# 設定天數的地方找回來了！
st.sidebar.header("分析設定")
days_to_check = st.sidebar.slider("比對過去幾天？", 2, 5, 2)

@st.cache_data(ttl=600)
def get_stock_data(date_str):
    headers = {'User-Agent': 'Mozilla/5.0'}
    url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={date_str}&selectType=ALL"
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200 and resp.json().get('stat') == 'OK':
            return pd.DataFrame(resp.json()['data'], columns=resp.json()['fields'])
    except: return None
    return None

if st.button("🔍 啟動全方位自動分析"):
    all_dfs = []
    curr = datetime.now()
    attempts = 0
    progress_bar = st.progress(0)
    
    with st.spinner("正在努力連線證交所..."):
        while len(all_dfs) < days_to_check and attempts < 10:
            d_str = curr.strftime("%Y%m%d")
            df = get_stock_data(d_str)
            if df is not None:
                all_dfs.append(df)
                st.write(f"✅ 已抓取 {d_str} 數據")
            curr -= timedelta(days=1)
            attempts += 1
            progress_bar.progress(len(all_dfs) / days_to_check)
            time.sleep(1) # 休息一下避免被鎖
            
        if len(all_dfs) >= 2:
            base = all_dfs[0].copy()
            # 這裡幫你做連連看
            for other in all_dfs[1:]:
                base = base[base['證券代號'].isin(other['證券代號'])]
            
            st.success(f"成功！找到連續 {len(all_dfs)} 天進榜的真愛股：")
            st.dataframe(base, use_container_width=True)
        else:
            st.warning("目前證交所比較忙，請重新按一次按鈕試試！")
