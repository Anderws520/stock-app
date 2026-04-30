import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import time

st.set_page_config(page_title="老周法人真愛分析 (Google 加速版)", layout="wide")
st.title("🚀 老周法人真愛股 - 穩定連線版")

# 💡 老周！請把下面這行換成你剛才在 Google 複製的那個網址
GAS_URL = "你的_GOOGLE_腳本_部署網址"

st.sidebar.header("分析設定")
days_to_check = st.sidebar.slider("比對過去幾天？", 2, 5, 2)

def get_data_via_gas(date_str):
    try:
        # 改成找 Google 要資料，避開證交所封鎖
        resp = requests.get(f"{GAS_URL}?date={date_str}", timeout=20)
        if resp.status_code == 200:
            json_data = resp.json()
            if json_data.get('stat') == 'OK':
                return pd.DataFrame(json_data['data'], columns=json_data['fields'])
    except:
        return None
    return None

if st.button("🔍 啟動 Google 穩定分析"):
    all_dfs = []
    # Google 版很穩，我們可以直接從今天開始抓
    curr = datetime.now()
    attempts = 0
    
    with st.spinner("正在透過 Google 中繼站調取資料..."):
        while len(all_dfs) < days_to_check and attempts < 10:
            d_str = curr.strftime("%Y%m%d")
            df = get_data_via_gas(d_str)
            
            if df is not None:
                all_dfs.append(df)
                st.write(f"✅ {d_str} 透過 Google 抓取成功！")
                # 既然透過 Google，裝死時間可以縮短，1秒就夠了
                time.sleep(1)
            else:
                st.write(f"ℹ️ {d_str} 無資料（可能是假日）")
            
            curr -= timedelta(days=1)
            attempts += 1
            
        if len(all_dfs) >= 2:
            base = all_dfs[0].copy()
            for other in all_dfs[1:]:
                base = base[base['證券代號'].isin(other['證券代號'])]
            st.success(f"老周，這次真的穩了！連續 {len(all_dfs)} 天名單：")
            st.dataframe(base, use_container_width=True)
        else:
            st.error("連 Google 中繼站都找不到資料，建議確認今日是否為交易日。")
