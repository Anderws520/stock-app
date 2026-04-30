import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import time
import random

st.set_page_config(page_title="老周法人真愛分析", layout="wide")
st.title("🚀 老周法人真愛股 + 集保大數據")

# 拉霸裝回來了
st.sidebar.header("分析設定")
days_to_check = st.sidebar.slider("比對過去幾天？", 2, 5, 2)

def get_stock_data(date_str):
    # 使用更強大的偽裝標頭
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://www.twse.com.tw/zh/page/trading/fund/T86.html'
    }
    # 加上隨機參數，讓網址每次都不同，騙過防火牆
    url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={date_str}&selectType=ALL&_={int(time.time()*1000)}"
    
    try:
        # 增加連線等待時間至 20 秒
        resp = requests.get(url, headers=headers, timeout=20)
        if resp.status_code == 200:
            json_data = resp.json()
            if json_data.get('stat') == 'OK':
                return pd.DataFrame(json_data['data'], columns=json_data['fields'])
    except:
        return None
    return None

if st.button("🔍 啟動全方位自動分析"):
    all_dfs = []
    # 💡 策略：避開今天(4/30)可能還沒穩定的資料，從昨天往前抓
    curr = datetime.now() - timedelta(days=1)
    attempts = 0
    
    status_text = st.empty()
    
    with st.spinner("正在跟證交所『躲貓貓』抓資料中..."):
        while len(all_dfs) < days_to_check and attempts < 7:
            d_str = curr.strftime("%Y%m%d")
            status_text.text(f"正在嘗試抓取 {d_str} 的資料...")
            
            df = get_stock_data(d_str)
            if df is not None:
                all_dfs.append(df)
                st.write(f"✅ {d_str} 抓取成功！")
                # 抓到後強制休息 5 秒，這就是你要的「裝死」
                time.sleep(5)
            else:
                st.write(f"❌ {d_str} 被擋或無資料，換一天試試...")
                time.sleep(2)
                
            curr -= timedelta(days=1)
            attempts += 1
            
        if len(all_dfs) >= 2:
            base = all_dfs[0].copy()
            for other in all_dfs[1:]:
                base = base[base['證券代號'].isin(other['證券代號'])]
            st.success(f"老周，終於突破重圍！連續 {len(all_dfs)} 天名單如下：")
            st.dataframe(base, use_container_width=True)
        else:
            st.error("⚠️ 雲端 IP 目前被證交所列入黑名單。建議先用 Pydroid 跑，或等一小時後再試。")
