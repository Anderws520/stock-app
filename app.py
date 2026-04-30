import streamlit as st
import pandas as pd
import requests
from datetime import datetime
import time
import re

st.set_page_config(page_title="專業操盤系統-抗干擾版", layout="wide")
st.title("🛡️ 買點定位系統 (100% 週末相容版)")

GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.sidebar.header("📅 交易時間軸")
start_date = st.sidebar.date_input("分析起始日期", datetime(2026, 4, 20))
end_date = st.sidebar.date_input("分析結束日期", datetime(2026, 4, 30))

def get_chips_safe(date_str):
    """強化版抓取：自動過濾無效數據與週末"""
    try:
        clean_date = date_str.replace("-", "")
        resp = requests.get(f"{GAS_URL}?date={clean_date}", timeout=10)
        if resp.status_code == 200:
            json_data = resp.json()
            # 關鍵修正：必須 stat 為 OK 且 data 裡面真的有東西才處理
            if json_data.get('stat') == 'OK' and json_data.get('data') and len(json_data['data']) > 0:
                df = pd.DataFrame(json_data['data'], columns=json_data['fields'])
                df.columns = [c.strip() for c in df.columns]
                df['日期'] = date_str
                return df
    except:
        pass
    return None

def get_price_live(stock_id):
    """原生獲取即時價格"""
    try:
        url = f"https://www.google.com/finance/quote/{stock_id}:TPE"
        headers = {'User-Agent': 'Mozilla/5.0'}
        resp = requests.get(url, headers=headers, timeout=5)
        price_match = re.search(r'data-last-price="([\d\.]+)"', resp.text)
        if price_match:
            return float(price_match.group(1))
    except:
        pass
    return None

if st.button("🚀 啟動抗干擾數據分析"):
    date_list = pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d").tolist()
    valid_dfs = []
    
    with st.spinner("正在逐日校準數據 (自動跳過非交易日)..."):
        p_bar = st.progress(0)
        for i, d in enumerate(date_list):
            df = get_chips_safe(d)
            if df is not None:
                valid_dfs.append(df)
            p_bar.progress((i + 1) / len(date_list))
            time.sleep(0.05)

    if valid_dfs:
        full_df = pd.concat(valid_dfs, ignore_index=True)
        
        # 數值清洗
        f_col = '外陸資買賣超股數(不含外資自營商)'
        i_col = '投信買賣超股數'
        def to_n(v): return pd.to_numeric(str(v).replace(',',''), errors='coerce') or 0

        # 以最後一個有交易的日期作為基準
        last_trading_date = full_df['日期'].max()
        latest_subset = full_df[full_df['日期'] == last_trading_date].copy()
        
        latest_subset['Buy_Vol'] = latest_subset.apply(lambda r: (to_n(r[f_col]) + to_n(r[i_col])) / 1000, axis=1)
        
        # 篩選核心標的
        top_stocks = latest_subset[latest_subset['Buy_Vol'] > 100].sort_values('Buy_Vol', ascending=False).head(20)

        results = []
        with st.spinner(f"正在對齊最後交易日 ({last_trading_date}) 價格..."):
            for _, row in top_stocks.iterrows():
                sid = row['證券代號']
                price = get_price_live(sid)
                
                # 計算在這個區間內，該股出現了幾次 (代表法人買超天數)
                continuity = len(full_df[full_df['證券代號'] == sid]['日期'].unique())
                
                results.append({
                    '代號': sid,
                    '名稱': row['證券名稱'],
                    '法人合計買超(張)': int(row['Buy_Vol']),
                    '目前參考價': price if price else "查無價格",
                    '區間出現天數': f"{continuity} 天",
                    '最後交易日': last_trading_date,
                    '操盤建議': "💎 強勢連買" if continuity >= 3 else "✨ 新進榜單"
                })

        if results:
            st.success(f"✅ 成功跳過週末，並對齊 {len(valid_dfs)} 個交易日數據！")
            st.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True)
        else:
            st.warning("所選區間內，法人買超未達門檻。")
    else:
        st.error("❌ 您選擇的日期區間內似乎沒有任何交易日資料，請重新檢查。")
