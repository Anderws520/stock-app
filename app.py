import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import time

st.set_page_config(page_title="法人真愛分析 - 終極對齊版", layout="wide")
st.title("🛡️ 買點定位系統 - 證交所標準格式版")

GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.sidebar.header("📅 戰略分析區間設定")
start_date = st.sidebar.date_input("開始日期", datetime(2026, 4, 20))
end_date = st.sidebar.date_input("結束日期", datetime(2026, 4, 30))

def get_data(date_str):
    try:
        resp = requests.get(f"{GAS_URL}?date={date_str}&t={time.time()}", timeout=30)
        if resp.status_code == 200:
            json_data = resp.json()
            if json_data.get('stat') == 'OK':
                df = pd.DataFrame(json_data['data'], columns=json_data['fields'])
                # 強制清理欄位名稱中的隱形空白
                df.columns = [c.strip() for c in df.columns]
                df['日期'] = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}"
                
                # 證交所官方標準欄位對照
                p_col = '收盤價' if '收盤價' in df.columns else '價格'
                
                # 數值清理轉換
                target_nums = ['外陸資買賣超股數(不含外資自營商)', '投信買賣超股數', '自營商買賣超股數', p_col]
                for col in target_nums:
                    if col in df.columns:
                        df[col] = df[col].astype(str).str.replace(',','').replace('', '0').astype(float)
                
                # 計算買超張數
                df['外資張'] = (df['外陸資買賣超股數(不含外資自營商)'] / 1000).round(0)
                df['投信張'] = (df['投信買賣超股數'] / 1000).round(0)
                df['合計買超'] = df['外資張'] + df['投信張'] + (df.get('自營商買賣超股數', 0)/1000).round(0)
                
                return df, p_col
    except: return None, None
    return None, None

if st.button("🔍 執行全方位掃描"):
    date_range = pd.date_range(start=start_date, end=end_date).strftime("%Y%m%d").tolist()
    all_raw_data = []
    final_p_col = '收盤價'
    
    with st.spinner("正在校準證交所官方數據欄位..."):
        for d_str in date_range:
            df, p_col = get_data(d_str)
            if df is not None and not df.empty:
                all_raw_data.append(df)
                if p_col: final_p_col = p_col
            time.sleep(0.2)
            
        if len(all_raw_data) >= 1:
            full_df = pd.concat(all_raw_data)
            results = []
            
            for stock_id, group in full_df.groupby('證券代號'):
                group = group.sort_values('日期')
                
                # 計算 5 日均價與現價
                prices = group[final_p_col].tolist() if final_p_col in group.columns else []
                current_p = prices[-1] if prices else 0
                avg_5p = sum(prices[-5:]) / len(prices[-5:]) if prices else 0
                diff_p = ((current_p - avg_5p) / avg_5p) if avg_5p != 0 else 0
                
                # 判定建議與最佳購買日期
                strong_days = group[(group['外資張'] > 0) & (group['投信張'] > 0)]
                first_date = strong_days.iloc[0]['日期'] if not strong_days.empty else group.iloc[0]['日期']
                advice = "💎 雙強初現(首選)" if len(group) <= 2 else "✅ 穩定增溫"
                
                last_info = group.iloc[-1]
                
                # 按照 Excel 截圖 A 到 L 欄順序排列
                results.append({
                    '日期': last_info['日期'],              # A
                    '股票代號': stock_id,                  # B
                    '股票名稱': last_info['證券名稱'],       # C
                    '關鍵分點': '三大法人',                # D
                    '買超張數': int(last_info['合計買超']),   # E
                    '5日均價': round(avg_5p, 2),            # F
                    '目前現價': round(current_p, 2),         # G
                    '價差%': f"{diff_p:.2%}",               # H
                    '連續出現天數': len(group),             # I
                    '集保人數變動': '無數據',               # J
                    '最佳購買日期': first_date,             # K
                    '操盤建議': advice,                    # L
                    'rank': 0 if advice == "💎 雙強初現(首選)" else 1
                })
            
            final_df = pd.DataFrame(results).sort_values(by=['rank', '連續出現天數', '買超張數'], ascending=[True, False, False])
            st.success(f"數據已完全校準並按指定格式排列。")
            st.dataframe(final_df.drop(columns=['rank']), use_container_width=True, hide_index=True)
