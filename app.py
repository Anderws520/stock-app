import streamlit as st
import pandas as pd
import requests
from datetime import datetime
import time

st.set_page_config(page_title="買點定位系統-終極版", layout="wide")
st.title("🛡️ 買點定位系統 (專業邏輯校準版)")

GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.sidebar.header("📅 戰略分析區間")
start_date = st.sidebar.date_input("開始日期", datetime(2026, 4, 20))
end_date = st.sidebar.date_input("結束日期", datetime(2026, 4, 30))

def get_data(date_str):
    try:
        resp = requests.get(f"{GAS_URL}?date={date_str}&t={time.time()}", timeout=30)
        if resp.status_code == 200:
            json_data = resp.json()
            if json_data.get('stat') == 'OK':
                df = pd.DataFrame(json_data['data'], columns=json_data['fields'])
                df.columns = [c.strip() for c in df.columns]
                df['日期'] = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}"
                
                # 解決價格為 0 的核心修正：鎖定證交所標準欄位
                # 遍歷可能的欄位名稱，確保抓到真正的收盤價
                p_col = next((c for c in df.columns if c in ['收盤價', '價格', '成交價']), None)
                
                # 強制數值化，處理千分位逗號
                target_cols = ['外陸資買賣超股數(不含外資自營商)', '投信買賣超股數', '自營商買賣超股數']
                if p_col: target_cols.append(p_col)
                
                for col in target_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col].astype(str).str.replace(',',''), errors='coerce').fillna(0)
                
                # 將股數換算為張數
                df['外資張'] = (df['外陸資買賣超股數(不含外資自營商)'] / 1000).round(0)
                df['投信張'] = (df['投信買賣超股數'] / 1000).round(0)
                df['合計買超'] = df['外資張'] + df['投信張'] + (df.get('自營商買賣超股數', 0)/1000).round(0)
                
                return df, p_col
    except: return None, None
    return None, None

if st.button("🔍 執行「獲利潛力」掃描 (限額20名)"):
    date_range = pd.date_range(start=start_date, end=end_date).strftime("%Y%m%d").tolist()
    all_raw_data = []
    actual_price_col = None
    
    with st.spinner("正在進行多重籌碼過濾與價格校準..."):
        for d_str in date_range:
            df, p_col = get_data(d_str)
            if df is not None and not df.empty:
                all_raw_data.append(df)
                if p_col: actual_price_col = p_col
            time.sleep(0.1)
            
        if all_raw_data:
            full_df = pd.concat(all_raw_data)
            results = []
            
            for stock_id, group in full_df.groupby('證券代號'):
                group = group.sort_values('日期')
                
                # 獲取價格序列，計算 5 日均價
                prices = group[actual_price_col].tolist() if actual_price_col in group.columns else []
                current_p = prices[-1] if prices else 0
                avg_5p = sum(prices[-5:]) / len(prices[-5:]) if prices else 0
                diff_p = ((current_p - avg_5p) / avg_5p) if avg_5p != 0 else 0
                
                # 核心建議邏輯：區分「初現」、「鎖碼」與「趨勢」
                last_info = group.iloc[-1]
                count = len(group)
                
                # 首選條件：最新日法人同買，且連續出現天數短 (初次發動)
                is_both_buy = (last_info['外資張'] > 0) and (last_info['投信張'] > 0)
                
                if is_both_buy and count <= 2:
                    advice = "💎 雙強初現(首選)"
                    rank_score = 1
                elif last_info['投信張'] > 500:
                    advice = "🔥 投信鎖碼(波段)"
                    rank_score = 2
                else:
                    advice = "✅ 趨勢跟蹤"
                    rank_score = 3
                
                results.append({
                    '日期': last_info['日期'],              # A
                    '股票代號': stock_id,                  # B
                    '股票名稱': last_info['證券名稱'],       # C
                    '關鍵分點': '三大法人',                # D
                    '買超張數': int(last_info['合計買超']),   # E
                    '5日均價': round(avg_5p, 2),            # F
                    '目前現價': round(current_p, 2),         # G
                    '價差%': f"{diff_p:.2%}",               # H
                    '連續出現天數': count,                 # I
                    '集保人數變動': '無數據',               # J
                    '最佳購買日期': group.iloc[0]['日期'],    # K
                    '操盤建議': advice,                    # L
                    'score': rank_score
                })
            
            # 依照「建議等級」與「買超張數」進行最終排序，並取前 20
            final_df = pd.DataFrame(results).sort_values(by=['score', '買超張數'], ascending=[True, False]).head(20)
            st.dataframe(final_df.drop(columns=['score']), use_container_width=True, hide_index=True)
