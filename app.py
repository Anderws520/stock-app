import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import time

st.set_page_config(page_title="老周法人真愛分析 終極版", layout="wide")
st.title("🛡️ 操盤手終極版 - 買點定位系統")

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
                df['日期'] = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}"
                df.columns = [c.strip() for c in df.columns]
                
                # 自動偵測價格欄位並轉換
                price_col = '收盤價' if '收盤價' in df.columns else '價格' 
                num_cols = ['外陸資買賣超股數(不含外資自營商)', '投信買賣超股數', '自營商買賣超股數', price_col]
                
                for col in num_cols:
                    if col in df.columns:
                        df[col] = df[col].astype(str).str.replace(',','').replace('', '0').astype(float)
                
                df['外資張'] = (df['外陸資買賣超股數(不含外資自營商)'] / 1000).round(0)
                df['投信張'] = (df['投信買賣超股數'] / 1000).round(0)
                df['合計買超'] = df['外資張'] + df['投信張'] + (df['自營商買賣超股數']/1000).round(0)
                
                return df, price_col
    except: return None, None
    return None, None

if st.button("🔍 執行全方位掃描"):
    date_range = pd.date_range(start=start_date, end=end_date).strftime("%Y%m%d").tolist()
    all_raw_data = []
    actual_price_col = '收盤價'
    
    with st.spinner("正在校準數據..."):
        for d_str in date_range:
            df, p_col = get_data(d_str)
            if df is not None and not df.empty:
                all_raw_data.append(df)
                actual_price_col = p_col
            time.sleep(0.2)
            
        if len(all_raw_data) >= 1:
            full_df = pd.concat(all_raw_data)
            max_date = full_df['日期'].max()
            
            results = []
            for stock_id, group in full_df.groupby('證券代號'):
                group = group.sort_values('日期')
                
                # 計算價格指標
                prices = group[actual_price_col].tolist()
                current_p = prices[-1]
                avg_5p = sum(prices[-5:]) / len(prices[-5:]) if prices else 0
                diff_p = ((current_p - avg_5p) / avg_5p * 100) if avg_5p != 0 else 0
                
                # 買點與操盤建議
                strong_days = group[(group['外資張'] > 0) & (group['投信張'] > 0)]
                first_date = strong_days.iloc[0]['日期'] if not strong_days.empty else group.iloc[0]['日期']
                advice = "💎 雙強初現(首選)" if len(group) <= 2 else "✅ 穩定增溫"
                
                last_info = group.iloc[-1]
                # 依照圖片 L 欄順序排列
                results.append({
                    '日期': last_info['日期'],
                    '股票代號': stock_id,
                    '股票名稱': last_info['證券名稱'],
                    '關鍵分點': '三大法人',
                    '買超張數': int(last_info['合計買超']),
                    '5日均價': round(avg_5p, 2),
                    '目前現價': round(current_p, 2),
                    '價差%': f"{diff_p:.2f}%",
                    '連續出現天數': len(group),
                    '集保人數變動': '無數據', # 需串接集保 API 暫提供預留
                    '最佳購買日期': first_date,
                    '操盤建議': advice,
                    'rank': 0 if advice == "💎 雙強初現(首選)" else 1
                })
            
            # 依照「首選」排序並顯示
            final_df = pd.DataFrame(results).sort_values(by=['rank', '連續出現天數'], ascending=[True, False])
            st.success(f"老周，已依照指定格式對齊！")
            st.dataframe(final_df.drop(columns=['rank']), use_container_width=True, hide_index=True)
