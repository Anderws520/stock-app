import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import time

st.set_page_config(page_title="老周法人真愛分析 3.0", layout="wide")
st.title("🛡️ 操盤手終極版 - 買點定位系統")

GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.sidebar.header("📅 戰略分析區間")
start_date = st.sidebar.date_input("開始日期", datetime.now() - timedelta(days=14)) # 建議選長一點
end_date = st.sidebar.date_input("結束日期", datetime.now())

def get_data(date_str):
    try:
        resp = requests.get(f"{GAS_URL}?date={date_str}&t={time.time()}", timeout=30)
        if resp.status_code == 200:
            json_data = resp.json()
            if json_data.get('stat') == 'OK':
                df = pd.DataFrame(json_data['data'], columns=json_data['fields'])
                df['日期'] = date_str
                df.columns = [c.strip() for c in df.columns]
                
                for col in ['外陸資買賣超股數(不含外資自營商)', '投信買賣超股數', '自營商買賣超股數']:
                    df[col] = df[col].astype(str).str.replace(',','').replace('', '0').astype(float)
                
                df['外資張'] = (df['外陸資買賣超股數(不含外資自營商)'] / 1000).round(0)
                df['投信張'] = (df['投信買賣超股數'] / 1000).round(0)
                df['合計買超'] = df['外資張'] + df['投信張'] + (df['自營商買賣超股數']/1000).round(0)
                return df[df['合計買超'] > 100]
    except: return None
    return None

if st.button("📈 執行買點定位分析"):
    date_range = pd.date_range(start=start_date, end=end_date).strftime("%Y%m%d").tolist()
    all_raw_data = []
    
    with st.spinner("正在掃描歷史籌碼發動訊號..."):
        for d_str in date_range:
            df = get_data(d_str)
            if df is not None and not df.empty:
                all_raw_data.append(df)
            time.sleep(0.3)
            
        if len(all_raw_data) >= 1:
            full_df = pd.concat(all_raw_data)
            
            # --- 核心邏輯：計算每支股票的「最佳買點」 ---
            results = []
            for stock_id, group in full_df.groupby('證券代號'):
                group = group.sort_values('日期') # 照時間排
                
                # 1. 第一次出現雙強同買的日子
                strong_days = group[(group['外資張'] > 0) & (group['投信張'] > 0)]
                if not strong_days.empty:
                    best_date = strong_days.iloc[0]['日期']
                    advice = "💎 雙強初現(首選)"
                else:
                    best_date = group.iloc[0]['日期']
                    advice = "⚖️ 籌碼首日轉強"
                
                # 2. 如果最新一天買超張數暴增，建議為當日
                latest_day = group.iloc[-1]
                if len(group) > 1 and latest_day['合計買超'] > group.iloc[-2]['合計買超'] * 1.5:
                    best_date = latest_day['日期']
                    advice = "🚀 買力爆發日"
                
                # 整理該股票在最新一天的資訊
                last_info = group.iloc[-1]
                results.append({
                    '日期': last_info['日期'],
                    '代號': stock_id,
                    '名稱': last_info['證券名稱'],
                    '今日合計': int(last_info['合計買超']),
                    '投信張': int(last_info['投信張']),
                    '連榜天數': len(group),
                    '最佳購買日期': f"{best_date[:4]}/{best_date[4:6]}/{best_date[6:]}",
                    '操盤建議': advice
                })
            
            final_df = pd.DataFrame(results)
            # 排序：日期最新 -> 連榜天數多 -> 買力
            final_df = final_df.sort_values(by=['日期', '連榜天數', '今日合計'], ascending=[False, False, False])
            
            st.success("老周，買點定位完成！重點關注『最佳購買日期』為今日或昨天的標的。")
            st.dataframe(final_df, use_container_width=True, hide_index=True)
        else:
            st.warning("查無符合籌碼條件的標的。")
