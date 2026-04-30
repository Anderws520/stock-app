import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import time
import re

st.set_page_config(page_title="專業操盤系統-原生穩定版", layout="wide")
st.title("🛡️ 買點定位系統 (原生數據校準版)")

# 您指定的穩定籌碼來源
GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.sidebar.header("📅 交易時間軸")
start_date = st.sidebar.date_input("分析起始日期", datetime(2026, 4, 20))
end_date = st.sidebar.date_input("分析結束日期", datetime(2026, 4, 30))

def get_chips(date_str):
    """獲取 GAS 籌碼"""
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

def get_price_native(stock_id):
    """原生爬取 Google Finance 價格 (無需額外套件)"""
    try:
        # 台灣市場代號處理
        market = "TPE" # 上市
        url = f"https://www.google.com/finance/quote/{stock_id}:{market}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        resp = requests.get(url, headers=headers, timeout=5)
        
        # 使用正則表達式快速定位價格
        # Google Finance 價格通常放在特定 class 內
        price_match = re.search(r'data-last-price="([\d\.]+)"', resp.text)
        if price_match:
            return float(price_match.group(1))
    except: pass
    return None

if st.button("🚀 啟動「買點定位」分析"):
    date_list = pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d").tolist()
    all_chips = []
    
    with st.spinner("正在連線 GAS 獲取法人籌碼..."):
        for d in date_list:
            df = get_chips(d)
            if df is not None:
                df['日期'] = d
                all_chips.append(df)
            time.sleep(0.1)

    if all_chips:
        full_chips = pd.concat(all_chips)
        
        # 提取最新一天的法人大買名單
        f_col = '外陸資買賣超股數(不含外資自營商)'
        i_col = '投信買賣超股數'
        
        def to_n(v): return pd.to_numeric(str(v).replace(',',''), errors='coerce') or 0
        
        latest_data = full_chips[full_chips['日期'] == date_list[-1]].copy()
        latest_data['Total_Buy_Vol'] = latest_data.apply(lambda r: (to_n(r[f_col]) + to_n(r[i_col])) / 1000, axis=1)
        
        # 篩選張數大於 100 且排序
        top_list = latest_data[latest_data['Total_Buy_Vol'] > 100].sort_values('Total_Buy_Vol', ascending=False).head(15)

        results = []
        with st.spinner("正在校準原生價格數據..."):
            for _, row in top_list.iterrows():
                sid = row['證券代號']
                price = get_price_native(sid)
                
                # 計算該股票在區間內出現的次數 (代表法人連續買超天數)
                continuity = len(full_chips[full_chips['證券代號'] == sid])
                
                results.append({
                    '股票': f"{sid} {row['證券名稱']}",
                    '法人買超(張)': int(row['Total_Buy_Vol']),
                    '目前成交價': price if price else "需手動確認",
                    '法人連買天數': continuity,
                    '建議': "💎 優選標的" if continuity >= 3 else "✨ 剛起步"
                })

        if results:
            st.success(f"✅ 成功對齊 {len(results)} 檔標的！")
            st.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True)
        else:
            st.warning("符合門檻的股票目前無資料。")
    else:
        st.error("GAS 連結無回應，請確認您的 Google 腳本權限。")
