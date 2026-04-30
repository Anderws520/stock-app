import os
import subprocess
import sys

# 自動安裝缺失套件的邏輯
def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

try:
    import yfinance as yf
except ImportError:
    install('yfinance')
    import yfinance as yf

import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import time

st.set_page_config(page_title="專業操盤系統-終極穩定版", layout="wide")
st.title("🛡️ 買點定位系統 (GAS籌碼 + 自動價格對齊)")

# 你指定的穩定籌碼來源
GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.sidebar.header("📅 交易時間軸")
start_date = st.sidebar.date_input("分析起始日期", datetime(2026, 4, 20))
end_date = st.sidebar.date_input("分析結束日期", datetime(2026, 4, 30))

def get_chips(date_str):
    try:
        clean_date = date_str.replace("-", "")
        resp = requests.get(f"{GAS_URL}?date={clean_date}", timeout=10)
        if resp.status_code == 200:
            json_data = resp.json()
            if json_data.get('stat') == 'OK' and json_data.get('data'):
                df = pd.DataFrame(json_data['data'], columns=json_data['fields'])
                df.columns = [c.strip() for c in df.columns]
                return df
    except: pass
    return None

if st.button("🚀 執行強勢股「買點定位」掃描"):
    date_list = pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d").tolist()
    all_chips = []
    
    with st.spinner("正在連線 GAS 獲取法人籌碼..."):
        p_bar = st.progress(0)
        for i, d in enumerate(date_list):
            df = get_chips(d)
            if df is not None:
                df['日期'] = d
                all_chips.append(df)
            p_bar.progress((i + 1) / len(date_list))
            time.sleep(0.1)

    if all_chips:
        full_chips = pd.concat(all_chips)
        # 篩選法人有動作的標的
        f_col = '外陸資買賣超股數(不含外資自營商)'
        i_col = '投信買賣超股數'
        
        def to_n(v): return pd.to_numeric(str(v).replace(',',''), errors='coerce') or 0
        
        # 取得最後一天大買的清單
        last_day = full_chips[full_chips['日期'] == date_list[-1]].copy()
        last_day['Total_Buy'] = last_day.apply(lambda r: (to_n(r[f_col]) + to_n(r[i_col])) / 1000, axis=1)
        top_picks = last_day[last_day['Total_Buy'] > 100].sort_values('Total_Buy', ascending=False).head(20)

        results = []
        with st.spinner("正在同步 Yahoo 價格並計算買點..."):
            for _, row in top_picks.iterrows():
                sid = row['證券代號']
                # 抓取 Yahoo 價格
                try:
                    ticker = f"{sid}.TW"
                    # 多抓幾天確保能算出 MA5
                    price_data = yf.download(ticker, start=start_date - timedelta(days=10), end=end_date + timedelta(days=1), progress=False)
                    if not price_data.empty:
                        price_data['MA5'] = price_data['Close'].rolling(window=5).mean()
                        curr_p = round(float(price_data['Close'].iloc[-1]), 2)
                        ma5 = round(float(price_data['MA5'].iloc[-1]), 2)
                        diff = ((curr_p - ma5) / ma5) if ma5 != 0 else 0
                        
                        results.append({
                            '股票': f"{sid} {row['證券名稱']}",
                            '法人買超(張)': int(row['Total_Buy']),
                            '現價': curr_p,
                            '5日均價': ma5,
                            '價差%': f"{diff:.2%}",
                            '操盤建議': "💎 買點出現" if abs(diff) < 0.02 else "🔥 趨勢追蹤"
                        })
                except: continue

        if results:
            st.success("✅ 數據對齊成功！")
            st.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True)
        else:
            st.error("無法對齊價格數據，請縮短日期範圍再試一次。")
    else:
        st.error("GAS 來源無數據。")
