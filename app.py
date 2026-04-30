import streamlit as st
import pandas as pd
import requests
from datetime import datetime
import time

st.set_page_config(page_title="買點定位系統-最終版", layout="wide")
st.title("🛡️ 買點定位系統 (價格與建議邏輯修正版)")

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
                df.columns = [c.strip() for c in df.columns]
                df['日期'] = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}"
                
                # 核心修正：找到包含「價」的欄位並排除掉「差」
                p_col = next((c for c in df.columns if '價' in c and '差' not in c), None)
                
                # 強制轉換所有數值欄位
                target_nums = ['外陸資買賣超股數(不含外資自營商)', '投信買賣超股數', '自營商買賣超股數']
                if p_col: target_nums.append(p_col)
                
                for col in target_nums:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col].astype(str).str.replace(',',''), errors='coerce').fillna(0)
                
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
    
    with st.spinner("數據同步中..."):
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
                
                # 價格計算
                prices = group[final_p_col].tolist() if final_p_col in group.columns else []
                current_p = prices[-1] if prices else 0
                avg_5p = sum(prices[-5:]) / len(prices[-5:]) if prices else 0
                diff_p = ((current_p - avg_5p) / avg_5p) if avg_5p != 0 else 0
                
                # 修正建議邏輯：不再盲目給予首選
                last_row = group.iloc[-1]
                count = len(group)
                
                if count <= 2 and last_row['合計買超'] > 500:
                    advice = "💎 雙強初現(首選)"
                elif count > 2 and last_row['合計買超'] > 0:
                    advice = "✅ 趨勢續強"
                else:
                    advice = "⚠️ 觀察等待"
                
                results.append({
                    '日期': last_row['日期'],
                    '股票代號': stock_id,
                    '股票名稱': last_row['證券名稱'],
                    '關鍵分點': '三大法人',
                    '買超張數': int(last_row['合計買超']),
                    '5日均價': round(avg_5p, 2),
                    '目前現價': round(current_p, 2),
                    '價差%': f"{diff_p:.2%}",
                    '連續出現天數': count,
                    '集保人數變動': '無數據',
                    '最佳購買日期': group.iloc[0]['日期'],
                    '操盤建議': advice
                })
            
            # 嚴格篩選前 20 檔
            final_df = pd.DataFrame(results).sort_values(by='買超張數', ascending=False).head(20)
            st.dataframe(final_df, use_container_width=True, hide_index=True)
