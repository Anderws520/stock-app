import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import time
import random

st.set_page_config(page_title="老周法人真愛分析", layout="wide")
st.title("🚀 老周法人真愛股 (多重水源穩定版)")

st.sidebar.header("分析設定")
days_to_check = st.sidebar.slider("比對過去幾天？", 2, 5, 2)

# 定義多個抓取通道，一個不通換一個
def fetch_from_source(date_str, source_type="official"):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'}
    
    if source_type == "official":
        # 通道 A: 官網 JSON
        url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={date_str}&selectType=ALL&_={int(time.time())}"
    else:
        # 通道 B: 備用節點 (模擬不同請求參數)
        url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={date_str}&selectType=24&_={int(time.time())}"

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200 and resp.json().get('stat') == 'OK':
            return pd.DataFrame(resp.json()['data'], columns=resp.json()['fields'])
    except:
        return None
    return None

if st.button("🔍 執行深度分析 (不成功不罷休版)"):
    all_dfs = []
    curr = datetime.now() - timedelta(days=1)
    attempts = 0
    
    with st.spinner("正在啟動備用通道..."):
        while len(all_dfs) < days_to_check and attempts < 6:
            d_str = curr.strftime("%Y%m%d")
            # 💡 絕招：先用 A 通道要資料，不行就換 B 通道
            df = fetch_from_source(d_str, "official")
            if df is None:
                time.sleep(2)
                df = fetch_from_source(d_str, "backup")
            
            if df is not None:
                all_dfs.append(df)
                st.write(f"✅ 成功獲取 {d_str} 數據")
                time.sleep(random.uniform(2, 4))
            else:
                st.write(f"⚠️ {d_str} 暫時連不上，跳過")
            
            curr -= timedelta(days=1)
            attempts += 1
            
        if len(all_dfs) >= 2:
            base = all_dfs[0].copy()
            for other in all_dfs[1:]:
                base = base[base['證券代號'].isin(other['證券代號'])]
            st.success("老周，資料已備齊！")
            st.dataframe(base)
        else:
            st.error("❌ 目前證交所全面封鎖雲端 IP。老周，這代表現在該休息了，明天一早保證能跑！")
