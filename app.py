import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import time
import yfinance as yf  # 這是抓取價格最穩定且不易被擋的方式

st.set_page_config(page_title="專業操盤系統-終極對齊版", layout="wide")
st.title("🛡️ 買點定位系統 (籌碼+價格 雙源對齊版)")

GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.sidebar.header("📅 交易時間軸")
start_date = st.sidebar.date_input("分析起始日期", datetime(2026, 4, 20))
end_date = st.sidebar.date_input("分析結束日期", datetime(2026, 4, 30))

def get_chips_from_gas(date_str):
    """從您指定的 GAS 連結獲取法人籌碼"""
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

def get_price_from_yahoo(stock_id, start, end):
    """從 Yahoo Finance 補齊價格與計算 MA5"""
    try:
        # 台灣股票需加上 .TW (上市) 或 .TWO (上櫃)
        ticker = f"{stock_id}.TW"
        # 抓取比 start_date 更早一點的資料以計算 MA5
        adj_start = start - timedelta(days=10)
        data = yf.download(ticker, start=adj_start, end=end + timedelta(days=1), progress=False)
        if not data.empty:
            data['MA5'] = data['Close'].rolling(window=5).mean()
            return data
    except: pass
    return None

if st.button("🚀 執行強勢股「買點定位」掃描"):
    date_list = pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d").tolist()
    all_chips = []
    
    with st.spinner("正在抓取法人籌碼數據..."):
        p_bar = st.progress(0)
        for i, d in enumerate(date_list):
            df = get_chips_from_gas(d)
            if df is not None:
                df['日期'] = d
                all_chips.append(df)
            p_bar.progress((i + 1) / len(date_list))
            time.sleep(0.1)

    if all_chips:
        full_chips = pd.concat(all_chips)
        # 取得不重複的股票代號清單 (前 50 名大買超，節省抓取時間)
        top_ids = full_chips.groupby('證券代號')['外陸資買賣超股數(不含外資自營商)'].sum().sort_values(ascending=False).head(50).index.tolist()
        
        results = []
        with st.spinner("正在對齊歷史價格與計算 MA5..."):
            for sid in top_ids:
                price_df = get_price_from_yahoo(sid, start_date, end_date)
                group = full_chips[full_chips['證券代號'] == sid].sort_values('日期')
                
                if price_df is not None and not group.empty:
                    last_date = group.iloc[-1]['日期']
                    # 獲取最後一天的價格與 MA5
                    try:
                        latest_p_row = price_df.iloc[-1]
                        curr_p = round(float(latest_p_row['Close']), 2)
                        ma5 = round(float(latest_p_row['MA5']), 2)
                        
                        # 計算法人買超張數
                        def clean_n(v): return pd.to_numeric(str(v).replace(',',''), errors='coerce') or 0
                        f_buy = clean_n(group.iloc[-1]['外陸資買賣超股數(不含外資自營商)']) / 1000
                        i_buy = clean_n(group.iloc[-1]['投信買賣超股數']) / 1000
                        total_buy = int(f_buy + i_buy)
                        
                        # 核心判定
                        diff_pct = ((curr_p - ma5) / ma5) if ma5 != 0 else 0
                        advice = "💎 雙強初現" if total_buy > 500 and len(group) <= 2 else "🔥 趨勢續強"
                        
                        results.append({
                            '股票代號': sid,
                            '股票名稱': group.iloc[-1]['證券名稱'],
                            '法人買超(張)': total_buy,
                            '現價': curr_p,
                            '5日均價': ma5,
                            '價差%': f"{diff_pct:.2%}",
                            '出現天數': len(group),
                            '建議': advice
                        })
                    except: continue

        if results:
            final_df = pd.DataFrame(results).sort_values('法人買超(張)', ascending=False)
            st.success("✅ 籌碼與價格對齊成功！")
            st.dataframe(final_df, use_container_width=True, hide_index=True)
        else:
            st.error("❌ 籌碼與價格對齊失敗，請確認日期區間。")
    else:
        st.error("❌ GAS 數據源目前無回應，請確認連結是否正確。")
