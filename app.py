import streamlit as st
import pandas as pd
import requests
from datetime import datetime
import time
import re

st.set_page_config(page_title="買點定位系統", layout="wide")
st.title("🛡️ 買點定位系統")

GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.sidebar.header("日期選擇")
start_date = st.sidebar.date_input("開始日期", datetime(2026, 4, 20))
end_date = st.sidebar.date_input("結束日期", datetime(2026, 4, 30))

def to_num(v):
    try:
        # 處理含逗號或空格的字串，轉為浮點數
        return float(str(v).replace(',', '').replace(' ', '').strip())
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

if st.button("執行全樣本分析"):
    date_list = pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d").tolist()
    all_dfs = []
    
    prog = st.progress(0)
    msg = st.empty()
    
    # 1. 抓取區間內所有原始資料
    for i, d in enumerate(date_list):
        msg.text(f"正在讀取 {d} 原始資料...")
        try:
            r = requests.get(f"{GAS_URL}?date={d.replace('-', '')}", timeout=10)
            json_data = r.json()
            if json_data.get('stat') == 'OK' and json_data.get('data'):
                df = pd.DataFrame(json_data['data'], columns=json_data['fields'])
                df.columns = [c.strip() for c in df.columns]
                df['分析日期'] = d
                all_dfs.append(df)
        except: pass
        prog.progress((i + 1) / len(date_list))
    
    if all_dfs:
        full_df = pd.concat(all_dfs, ignore_index=True)
        cols = full_df.columns.tolist()
        
        # 定義對應證交所的關鍵欄位 (image_e279f6.png)
        buy_col = next((c for c in cols if '三大法人買賣超股數' in c), None)
        price_col = next((c for c in cols if '收盤價' in c), None)
        
        # 鎖定最後一個交易日
        last_d = full_df['分析日期'].max()
        latest_stocks = full_df[full_df['分析日期'] == last_d].copy()
        
        results = []
        msg.text(f"正在執行全樣本計算 (共 {len(latest_stocks)} 檔)...")
        
        for _, row in latest_stocks.iterrows():
            sid = row['證券代號']
            
            # A-L 欄位邏輯計算
            total_buy_vol = round(to_num(row[buy_col]) / 1000, 0) if buy_col else 0
            
            s_hist = full_df[full_df['證券代號'] == sid]
            hist_prices = [to_num(x) for x in s_hist[price_col].tolist() if to_num(x) > 0] if price_col else []
            
            ma5 = round(sum(hist_prices) / len(hist_prices), 2) if hist_prices else 0
            curr_p = get_live_price(sid) or (hist_prices[-1] if hist_prices else 0)
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
                '集保人數變動': "查無數據",
                '最佳購買日期': last_d if -0.01 <= diff_pct <= 0.01 else "觀望",
                '操盤建議': "多頭排列" if curr_p > ma5 and total_buy_vol > 0 else "趨勢待定"
            })

        msg.empty()
        if results:
            final_df = pd.DataFrame(results)
            # 嚴格執行 image_e280a1.png 的 12 欄位順序輸出
            final_cols = ['日期', '股票代號', '股票名稱', '關鍵分點', '買超張數', '5日均價', '目前現價', '價差%', '連續出現天數', '集保人數變動', '最佳購買日期', '操盤建議']
            
            st.success(f"✅ 已完成全樣本分析。基準日：{last_d}，總計顯示 {len(results)} 檔股票。")
            st.dataframe(
                final_df[final_cols].sort_values('買超張數', ascending=False), 
                use_container_width=True, 
                hide_index=True
            )
        else:
            st.warning("分析過程中未產生有效數據。")
    else:
        st.error("無法取得任何交易數據，請確認日期區間是否正確。")
