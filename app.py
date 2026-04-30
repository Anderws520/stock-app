import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import time

st.set_page_config(page_title="老周法人真愛分析 (Google 版)", layout="wide")
st.title("🚀 老周法人真愛股 - 穩定不卡頓版")

# 💡 已經填入老周提供的 Google 中繼站網址
GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.sidebar.header("分析設定")
days_to_check = st.sidebar.slider("比對過去幾天？", 2, 5, 2)

def get_data_via_gas(date_str):
    try:
        # 改成找 Google 中繼站要資料，Google 的 IP 不會被證交所擋
        resp = requests.get(f"{GAS_URL}?date={date_str}", timeout=20)
        if resp.status_code == 200:
            json_data = resp.json()
            if json_data.get('stat') == 'OK':
                df = pd.DataFrame(json_data['data'], columns=json_data['fields'])
                return df
    except Exception as e:
        return None
    return None

if st.button("🔍 啟動 Google 穩定分析"):
    all_dfs = []
    # Google 版很穩，我們直接從今天開始抓
    curr = datetime.now()
    attempts = 0
    
    status_text = st.empty()
    progress_bar = st.progress(0)
    
    with st.spinner("正在透過 Google 傳送資料..."):
        while len(all_dfs) < days_to_check and attempts < 10:
            d_str = curr.strftime("%Y%m%d")
            status_text.text(f"正在請求 {d_str} 的資料...")
            
            df = get_data_via_gas(d_str)
            
            if df is not None:
                all_dfs.append(df)
                st.write(f"✅ {d_str} 透過 Google 抓取成功！")
                # 既然透過 Google，裝死時間可以縮短，0.5秒就夠了
                time.sleep(0.5)
            else:
                # 沒抓到通常是假日（週六日）
                pass
            
            curr -= timedelta(days=1)
            attempts += 1
            progress_bar.progress(min(attempts / 10, 1.0))
            
        if len(all_dfs) >= 2:
            base = all_dfs[0].copy()
            # 連連看邏輯：找出這幾天都有出現的證券代號
            for other in all_dfs[1:]:
                base = base[base['證券代號'].isin(other['證券代號'])]
            
            st.success(f"老周，大功告成！連續 {len(all_dfs)} 天進榜名單：")
            st.dataframe(base, use_container_width=True)
        else:
            st.error("目前的搜尋範圍內找不到足夠的資料，建議確認今天是否為開盤日。")
