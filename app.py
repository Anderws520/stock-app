import streamlit as st
import pandas as pd
import requests
import io
import urllib3
from datetime import datetime, timedelta

# 介面設定
st.set_page_config(page_title="老周法人選股", layout="wide")
st.title("🚀 老周法人真愛股分析 App")

@st.cache_data(ttl=3600)
def get_data(date_str):
    urllib3.disable_warnings()
    url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={date_str}&selectType=ALLBUT0999&response=csv"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        resp = requests.get(url, headers=headers, verify=False, timeout=10)
        if "查詢無資料" in resp.text: return None
        df = pd.read_csv(io.StringIO(resp.text), skiprows=1, encoding='big5', on_bad_lines='skip')
        df.columns = [c.strip() for c in df.columns]
        # 關鍵：買賣超股數轉張數
        df['買超張數'] = (df['三大法人買賣超股數'].astype(str).str.replace(',', '').astype(float) / 1000).round(0).astype(int)
        return df[['證券代號', '證券名稱', '買超張數']].sort_values(by='買超張數', ascending=False).head(20)
    except:
        return None

# 控制區
st.sidebar.header("分析設定")
check_days = st.sidebar.slider("比對過去幾天？", 3, 10, 5)

if st.button("🔍 開始自動分析"):
    all_dfs = []
    # 自動往前抓 N 天來比對「連續出現天數」
    for i in range(check_days + 5): # 多抓幾天確保能抓到足夠的交易日
        d = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
        df = get_data(d)
        if df is not None:
            all_dfs.append(df)
            if len(all_dfs) >= check_days: break
            
    if all_dfs:
        combined = pd.concat(all_dfs)
        counts = combined['證券代號'].value_counts()
        
        # 以最新一天為基準，加入連續天數
        final = all_dfs[0].copy()
        final['連續出現天數'] = final['證券代號'].map(counts)
        final = final.sort_values(['連續出現天數', '買超張數'], ascending=False)
        
        st.success("分析完成！")
        
        # 變色提醒：連續 3 天以上變紅色
        def highlight_strong(val):
            return 'background-color: #FF4B4B; color: white' if val >= 3 else ''
            
        st.dataframe(final.style.applymap(highlight_strong, subset=['連續出現天數']), use_container_width=True)
    else:
        st.error("暫無資料，請稍後再試。")
