import streamlit as st
import pandas as pd
import requests
from datetime import datetime
import time
import re

st.set_page_config(page_title="買點定位系統", layout="wide")
st.title("🛡️ 買點定位系統 (證交所對齊版)")

GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.sidebar.header("日期選擇")
start_date = st.sidebar.date_input("開始日期", datetime(2026, 4, 20))
end_date = st.sidebar.date_input("結束日期", datetime(2026, 4, 30))

def to_num(v):
    try:
        return float(str(v).replace(',', '').strip())
    except:
        return 0.0

def get_live_price(sid):
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        url = f"https://www.google.com/finance/quote/{sid}:TPE"
        r = requests.get(url, headers=headers, timeout=5)
        m = re.search(r'data-last-price="([\d\.]+)"', r.text)
        if m: return float(m.group(1))
    except: pass
    return None

if st.button("執行全欄位分析"):
    dates = pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d").tolist()
    all_dfs = []
    
    prog = st.progress(0)
    for i, d in enumerate(dates):
        try:
            r = requests.get(f"{GAS_URL}?date={d.replace('-', '')}", timeout=10)
            json_data = r.json()
            if json_data.get('stat') == 'OK' and json_data.get('data'):
                df = pd.DataFrame(json_data['data'], columns=json_data['fields'])
                df.columns = [c.strip() for c in df.columns]
                df['分析日期'] = d
                all_dfs.append(df)
        except: pass
        prog.progress((i + 1) / len(dates))
    
    if all_dfs:
        full_df = pd.concat(all_dfs, ignore_index=True)
        cols = full_df.columns.tolist()
        
        # 精確匹配證交所欄位名稱 (對應 image_e27cf9.png)
        total_buy_col = next((c for c in cols if '三大法人買賣超股數' in c), None)
        price_col = next((c for c in cols if '收盤價' in c), None)
        
        last_d = full_df['分析日期'].max()
        latest = full_df[full_df['分析日期'] == last_d].copy()
        
        results = []
        for _, row in latest.iterrows():
            sid = row['證券代號']
            
            # 1. 買超張數：對應最後一欄三大法人合計 (股轉張)
            total_buy_shares = to_num(row[total_buy_col]) if total_buy_col else 0
            total_buy_vol = round(total_buy_shares / 1000, 0)
            
            if total_buy_vol > 100: # 過濾門檻
                s_hist = full_df[full_df['證券代號'] == sid]
                
                # 2. 5日均價與現價
                hist_prices = [to_num(x) for x in s_hist[price_col].tolist() if to_num(x) > 0] if price_col else []
                ma5 = round(sum(hist_prices) / len(hist_prices), 2) if hist_prices else 0
                curr_p = get_live_price(sid) or (hist_prices[-1] if hist_prices else 0)
                
                # 3. 價差與天數
                diff_pct = (curr_p - ma5) / ma5 if ma5 > 0 else 0
                days = len(s_hist['分析日期'].unique())

                results.append({
                    '日期': last_d,
                    '股票代號': sid,
                    '股票名稱': row['證券名稱'],
                    '關鍵分點': "三大法人",
                    '買超張數': int(total_buy_vol),
                    '5日均價': ma5,
                    '目前現價': curr_p,
                    '價差%': f"{diff_pct:.2%}",
                    '連續出現天數': days,
                    '集保人數變動': "無數據",
                    '最佳購買日期': last_d if -0.01 <= diff_pct <= 0.02 else "觀察",
                    '操盤建議': "雙強初現" if total_buy_vol > 500 and days <= 2 else "趨勢續強"
                })

        if results:
            # 嚴格遵循 A-L 12欄位順序
            final_cols = ['日期', '股票代號', '股票名稱', '關鍵分點', '買超張數', '5日均價', '目前現價', '價差%', '連續出現天數', '集保人數變動', '最佳購買日期', '操盤建議']
            st.dataframe(pd.DataFrame(results)[final_cols].sort_values('買超張數', ascending=False), use_container_width=True, hide_index=True)
        else:
            st.warning("查無符合門檻標的。")
    else:
        st.error("日期區間內抓不到資料。")
