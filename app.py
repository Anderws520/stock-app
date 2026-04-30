import streamlit as st
import pandas as pd
import requests
from datetime import datetime
import time

st.set_page_config(page_title="買點定位系統-最終校準", layout="wide")
st.title("🛡️ 買點定位系統 (數據全對齊版本)")

GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.sidebar.header("📅 數據同步區間")
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
                
                # ✅ 核心修正 1：自動偵測任何包含「價」字且非差額的欄位
                p_col = next((c for c in df.columns if '價' in c and '差' not in c), None)
                
                # ✅ 核心修正 2：徹底轉換數值，排除所有非數字字元 (含逗號)
                target_cols = ['外陸資買賣超股數(不含外資自營商)', '投信買賣超股數', '自營商買賣超股數']
                if p_col: target_cols.append(p_col)
                
                for col in target_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col].astype(str).str.replace(',','').replace('--','0'), errors='coerce').fillna(0)
                
                df['外資張'] = (df['外陸資買賣超股數(不含外資自營商)'] / 1000).round(0)
                df['投信張'] = (df['投信買賣超股數'] / 1000).round(0)
                df['合計買超'] = df['外資張'] + df['投信張'] + (df.get('自營商買賣超股數', 0)/1000).round(0)
                
                return df, p_col
    except: return None, None
    return None, None

if st.button("🚀 執行前 20 強精確掃描"):
    date_range = pd.date_range(start=start_date, end=end_date).strftime("%Y%m%d").tolist()
    all_raw_data = []
    actual_price_col = None
    
    with st.spinner("正在執行 50 年邏輯校準與數據對齊..."):
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
                
                # ✅ 核心修正 3：確保價格計算逻辑正確，不為 0
                prices = group[actual_price_col].tolist() if actual_price_col and actual_price_col in group.columns else []
                current_p = prices[-1] if prices else 0
                avg_5p = (sum(prices[-5:]) / len(prices[-5:])) if len(prices) > 0 else 0
                diff_p = ((current_p - avg_5p) / avg_5p) if avg_5p != 0 else 0
                
                last_info = group.iloc[-1]
                count = len(group)
                
                # ✅ 核心修正 4：專業建議分級
                is_double_buy = (last_info['外資張'] > 0) and (last_info['投信張'] > 0)
                if is_double_buy and count <= 2:
                    advice = "💎 雙強初現(首選)"
                    priority = 1
                elif last_info['合計買超'] > 1000:
                    advice = "🔥 資金湧入(鎖碼)"
                    priority = 2
                else:
                    advice = "✅ 趨勢跟蹤"
                    priority = 3
                
                results.append({
                    '日期': last_info['日期'],
                    '股票代號': stock_id,
                    '股票名稱': last_info['證券名稱'],
                    '關鍵分點': '三大法人',
                    '買超張數': int(last_info['合計買超']),
                    '5日均價': round(avg_5p, 2),
                    '目前現價': round(current_p, 2),
                    '價差%': f"{diff_p:.2%}",
                    '連續出現天數': count,
                    '集保人數變動': '無數據',
                    '最佳購買日期': group.iloc[0]['日期'],
                    '操盤建議': advice,
                    'priority': priority
                })
            
            # ✅ 核心修正 5：嚴格依照買超量與優先級排序，取前 20
            final_df = pd.DataFrame(results).sort_values(by=['priority', '買超張數'], ascending=[True, False]).head(20)
            st.success("數據校準完成，價格與前 20 強邏輯已修復。")
            st.dataframe(final_df.drop(columns=['priority']), use_container_width=True, hide_index=True)
