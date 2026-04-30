import streamlit as st
import pandas as pd
import requests
from datetime import datetime
import time

st.set_page_config(page_title="買點定位系統-全標的版", layout="wide")
st.title("🛡️ 買點定位系統 (全標的前20強對齊版)")

GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.sidebar.header("📅 戰略分析設定")
start_date = st.sidebar.date_input("開始日期", datetime(2026, 4, 20))
end_date = st.sidebar.date_input("結束日期", datetime(2026, 4, 30))

def get_data(date_str):
    try:
        resp = requests.get(f"{GAS_URL}?date={date_str}&t={time.time()}", timeout=30)
        if resp.status_code == 200:
            json_data = resp.json()
            if json_data.get('stat') == 'OK':
                df = pd.DataFrame(json_data['data'], columns=json_data['fields'])
                # 強制清理所有欄位前後空白，防止 KeyError
                df.columns = [c.strip() for c in df.columns]
                df['日期'] = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}"
                
                # 自動偵測價格欄位 (個股/權證/ETF 通用)
                p_col = next((c for c in df.columns if '收盤' in c or '價格' in c), None)
                
                # 數值清理轉換
                target_nums = ['外陸資買賣超股數(不含外資自營商)', '投信買賣超股數', '自營商買賣超股數']
                if p_col: target_nums.append(p_col)
                
                for col in target_nums:
                    if col in df.columns:
                        # 處理千分位逗號並轉為數字，確保價格不為 0
                        df[col] = pd.to_numeric(df[col].astype(str).str.replace(',',''), errors='coerce').fillna(0)
                
                # 計算買超張數
                df['外資張'] = (df['外陸資買賣超股數(不含外資自營商)'] / 1000).round(0)
                df['投信張'] = (df['投信買賣超股數'] / 1000).round(0)
                df['合計買超'] = df['外資張'] + df['投信張'] + (df.get('自營商買賣超股數', 0)/1000).round(0)
                
                return df, p_col
    except: return None, None
    return None, None

if st.button("🔍 執行全標的前 20 強掃描"):
    date_range = pd.date_range(start=start_date, end=end_date).strftime("%Y%m%d").tolist()
    all_raw_data = []
    final_p_col = None
    
    with st.spinner("正在分析個股、權證及 ETF 並計算價格指標..."):
        for d_str in date_range:
            df, p_col = get_data(d_str)
            if df is not None and not df.empty:
                all_raw_data.append(df)
                if p_col: final_p_col = p_col
            time.sleep(0.1)
            
        if all_raw_data:
            full_df = pd.concat(all_raw_data)
            results = []
            
            for stock_id, group in full_df.groupby('證券代號'):
                group = group.sort_values('日期')
                
                # 正確計算價格指標
                prices = group[final_p_col].tolist() if final_p_col and final_p_col in group.columns else []
                current_p = prices[-1] if prices else 0
                avg_5p = sum(prices[-5:]) / len(prices[-5:]) if prices else 0
                diff_p = ((current_p - avg_5p) / avg_5p) if avg_5p != 0 else 0
                
                # 判定建議 (加入買超力道判定)
                last_row = group.iloc[-1]
                # 必須是多方籌碼進駐且連續天數短才給予首選建議
                is_multi_buy = (last_row['合計買超'] > 100) and (len(group) <= 2)
                advice = "💎 雙強初現(首選)" if is_multi_buy else "✅ 穩定增溫"
                
                results.append({
                    '日期': last_row['日期'],              # A
                    '股票代號': stock_id,                  # B
                    '股票名稱': last_row['證券名稱'],       # C
                    '關鍵分點': '三大法人',                # D
                    '買超張數': int(last_row['合計買超']),   # E
                    '5日均價': round(avg_5p, 2),            # F
                    '目前現價': round(current_p, 2),         # G
                    '價差%': f"{diff_p:.2%}",               # H
                    '連續出現天數': len(group),             # I
                    '集保人數變動': '無數據',               # J
                    '最佳購買日期': group.iloc[0]['日期'],    # K
                    '操盤建議': advice                     # L
                })
            
            # 依據買超張數進行前 20 名篩選
            final_df = pd.DataFrame(results).sort_values(by='買超張數', ascending=False).head(20)
            st.success("分析完成，以下為買超張數前 20 名之全標的。")
            st.dataframe(final_df, use_container_width=True, hide_index=True)
