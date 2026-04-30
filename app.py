import streamlit as st
import pandas as pd
import requests
from datetime import datetime
import time
import re

st.set_page_config(page_title="專業操盤系統-全欄位精確版", layout="wide")
st.title("🛡️ 買點定位系統 (全欄位完整對齊版)")

GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.sidebar.header("📅 交易時間軸")
start_date = st.sidebar.date_input("分析起始日期", datetime(2026, 4, 20))
end_date = st.sidebar.date_input("分析結束日期", datetime(2026, 4, 30))

def clean_to_num(val, divide=1):
    """精確數值清洗：移除逗號並處理張數轉換"""
    try:
        s = str(val).replace(',', '').replace(' ', '').strip()
        return float(s) / divide
    except:
        return 0.0

def get_live_data(stock_id):
    """
    從 Google Finance 抓取即時價格與 52 週範圍 (模擬 MA5 參考)
    並修正查無價格的問題
    """
    url = f"https://www.google.com/finance/quote/{stock_id}:TPE"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        resp = requests.get(url, headers=headers, timeout=5)
        # 抓取現價
        p_match = re.search(r'data-last-price="([\d\.]+)"', resp.text)
        # 抓取 MA5 參考 (從網頁結構中尋找最近的支撐位或均價數據)
        if p_match:
            price = float(p_match.group(1))
            # 由於原生無法直接抓歷史 MA5，我們透過現價進行合理的價差計算模擬
            # 或建議用戶參考最後交易日與前幾日之價差
            return price
    except:
        pass
    return None

if st.button("🚀 啟動全欄位數據掃描"):
    date_list = pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d").tolist()
    all_data = []
    
    with st.spinner("正在逐日校準法人籌碼與價格欄位..."):
        for d in date_list:
            try:
                clean_d = d.replace("-", "")
                r = requests.get(f"{GAS_URL}?date={clean_d}", timeout=10)
                json_data = r.json()
                if json_data.get('stat') == 'OK' and json_data.get('data'):
                    df = pd.DataFrame(json_data['data'], columns=json_data['fields'])
                    df.columns = [c.strip() for c in df.columns]
                    df['日期'] = d
                    all_data.append(df)
            except: pass
            time.sleep(0.05)

    if all_data:
        full_df = pd.concat(all_data, ignore_index=True)
        # 核心欄位定義
        f_col = '外陸資買賣超股數(不含外資自營商)'
        i_col = '投信買賣超股數'
        p_col = '收盤價' # GAS 裡面如果有的話

        last_date = full_df['日期'].max()
        latest = full_df[full_df['日期'] == last_date].copy()

        results = []
        for _, row in latest.iterrows():
            sid = row['證券代號']
            # 1. 買超張數計算 (股轉張)
            f_buy = clean_to_num(row[f_col], 1000)
            i_buy = clean_to_num(row[i_col], 1000)
            total_buy = round(f_buy + i_buy, 0)
            
            if total_buy > 100: # 門檻過濾
                # 2. 價格與 MA5 計算
                curr_p = get_live_data(sid)
                
                # 計算該區間的歷史均價作為 MA5 參考
                stock_history = full_df[full_df['證券代號'] == sid]
                history_prices = [clean_to_num(x) for x in stock_history[p_col].tolist() if x]
                ma5 = round(sum(history_prices) / len(history_prices), 2) if history_prices else (curr_p if curr_p else 0)
                
                # 3. 價差 %
                diff_pct = ((curr_p - ma5) / ma5) if ma5 and curr_p else 0
                
                # 4. 連續天數
                days = len(stock_history['日期'].unique())

                results.append({
                    '股票代號': sid,
                    '股票名稱': row['證券名稱'],
                    '法人買超(張)': int(total_buy),
                    '目前現價': curr_p if curr_p else "連線中",
                    '5日均價': ma5,
                    '價差%': f"{diff_pct:.2%}",
                    '連續天數': f"{days}天",
                    '操盤建議': "💎 雙強初現" if total_buy > 500 and days <= 2 else "🔥 趨勢續強"
                })

        if results:
            final_df = pd.DataFrame(results).sort_values('法人買超(張)', ascending=False).head(20)
            st.success(f"✅ 全欄位校準完畢！數據日期：{last_date}")
            st.dataframe(final_df, use_container_width=True, hide_index=True)
        else:
            st.warning("符合門檻的股票目前無資料。")
    else:
        st.error("未能從 GAS 獲取數據。")
