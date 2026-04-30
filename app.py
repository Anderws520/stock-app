import streamlit as st
import pandas as pd
import requests
from datetime import datetime
import time
import re

st.set_page_config(page_title="買點定位系統", layout="wide")
st.title("🛡️ 買點定位系統 (12欄位全對齊版)")

GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

# 介面設定
st.sidebar.header("日期選擇")
start_date = st.sidebar.date_input("開始日期", datetime(2026, 4, 20))
end_date = st.sidebar.date_input("結束日期", datetime(2026, 4, 30))

def fetch_data(d_str):
    try:
        url = f"{GAS_URL}?date={d_str.replace('-', '')}"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            json_data = resp.json()
            if json_data.get('stat') == 'OK' and json_data.get('data'):
                df = pd.DataFrame(json_data['data'], columns=json_data['fields'])
                df.columns = [c.strip() for c in df.columns]
                df['日期'] = d_str
                return df
    except: pass
    return None

def get_price(sid):
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        url = f"https://www.google.com/finance/quote/{sid}:TPE"
        r = requests.get(url, headers=headers, timeout=5)
        m = re.search(r'data-last-price="([\d\.]+)"', r.text)
        if m: return float(m.group(1))
    except: pass
    return 0.0

if st.button("執行全欄位分析"):
    dates = pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d").tolist()
    all_dfs = []
    
    prog = st.progress(0)
    for i, d in enumerate(dates):
        df = fetch_data(d)
        if df is not None: all_dfs.append(df)
        prog.progress((i + 1) / len(dates))
    
    if all_dfs:
        full_df = pd.concat(all_dfs, ignore_index=True)
        f_col, i_col, p_col = '外陸資買賣超股數(不含外資自營商)', '投信買賣超股數', '收盤價'
        def to_num(v): return pd.to_numeric(str(v).replace(',', ''), errors='coerce') or 0
        
        last_d = full_df['日期'].max()
        latest = full_df[full_df['日期'] == last_d].copy()
        
        results = []
        for _, row in latest.iterrows():
            sid = row['證券代號']
            f_buy = to_num(row[f_col]) / 1000
            i_buy = to_num(row[i_col]) / 1000
            total_buy = round(f_buy + i_buy, 0)
            
            if total_buy > 100:
                s_hist = full_df[full_df['證券代號'] == sid]
                prices = [to_num(x) for x in s_hist[p_col].tolist() if to_num(x) > 0]
                
                ma5 = round(sum(prices) / len(prices), 2) if prices else 0
                curr_p = get_price(sid) or (prices[-1] if prices else 0)
                diff_pct = (curr_p - ma5) / ma5 if ma5 > 0 else 0
                days = len(s_hist['日期'].unique())

                # 完全參照 image_e2893b.png 的 12 個欄位順序與名稱
                results.append({
                    '日期': last_d,
                    '股票代號': sid,
                    '股票名稱': row['證券名稱'],
                    '關鍵分點': "三大法人",
                    '買超張數': int(total_buy),
                    '5日均價': ma5,
                    '目前現價': curr_p,
                    '價差%': f"{diff_pct:.2%}",
                    '連續出現天數': days,
                    '集保人數變動': "無數據",
                    '最佳購買日期': last_d if abs(diff_pct) < 0.01 else "觀望",
                    '操盤建議': "雙強初現" if total_buy > 500 and days <= 2 else "趨勢續強"
                })

        if results:
            # 確保輸出欄位順序與圖片完全一致
            final_cols = ['日期', '股票代號', '股票名稱', '關鍵分點', '買超張數', '5日均價', '目前現價', '價差%', '連續出現天數', '集保人數變動', '最佳購買日期', '操盤建議']
            st.dataframe(pd.DataFrame(results)[final_cols].sort_values('買超張數', ascending=False), use_container_width=True, hide_index=True)
        else:
            st.warning("無符合門檻標的。")
    else:
        st.error("區間內無交易數據。")
