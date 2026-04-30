import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import time

st.set_page_config(page_title="老周法人真愛分析 - 終極修復版", layout="wide")
st.title("🛡️ 操盤手終極版 - 買點定位系統 (修復斷層)")

GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.sidebar.header("📅 戰略分析設定")
start_date = st.sidebar.date_input("開始日期", datetime(2026, 4, 20)) # 包含 4/27-4/30
end_date = st.sidebar.date_input("結束日期", datetime(2026, 4, 30))

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
                df['合計買超'] = df['外資張'] + df['投信張'] + (df['自營商買賣超股數'].astype(float)/1000).round(0)
                
                # 篩選個股：排除 ETF 並要求投信有動作且合計買超 > 100
                is_stock = ~df['證券代號'].str.startswith('00')
                return df[is_stock & (df['投信張'] > 0) & (df['合計買超'] > 100)]
    except: return None
    return None

if st.button("📈 執行全量買點定位"):
    date_range = pd.date_range(start=start_date, end=end_date).strftime("%Y%m%d").tolist()
    all_raw_data = []
    
    with st.spinner("正在修復日期斷層，抓取 4/27-4/30 數據..."):
        for d_str in date_range:
            df = get_data(d_str)
            if df is not None and not df.empty:
                all_raw_data.append(df)
            time.sleep(0.2)
            
        if len(all_raw_data) >= 1:
            full_df = pd.concat(all_raw_data)
            max_date = full_df['日期'].max()
            
            results = []
            for stock_id, group in full_df.groupby('證券代號'):
                group = group.sort_values('日期')
                
                # 判定雙強初現：第一次外資投信同買且在最近 3 天內才開始
                strong_days = group[(group['外資張'] > 0) & (group['投信張'] > 0)]
                first_strong_date = strong_days.iloc[0]['日期'] if not strong_days.empty else group.iloc[0]['日期']
                
                # 狀態標籤
                is_first_buy = len(group) <= 2
                advice = "💎 雙強初現(首選)" if is_first_buy else "✅ 穩定增溫"
                
                last_info = group.iloc[-1]
                # 確保只秀出最新交易日的資料
                if last_info['日期'] == max_date:
                    results.append({
                        '日期': last_info['日期'],
                        '代號': stock_id,
                        '名稱': last_info['證券名稱'],
                        '今日合計': int(last_info['合計買超']),
                        '投信張': int(last_info['投信張']),
                        '連榜天數': len(group),
                        '最佳購買日期': f"{first_strong_date[:4]}/{first_strong_date[4:6]}/{first_strong_date[6:]}",
                        '操盤建議': advice,
                        'sort_rank': 0 if advice == "💎 雙強初現(首選)" else 1
                    })
            
            final_df = pd.DataFrame(results).sort_values(by=['sort_rank', '連榜天數', '今日合計'], ascending=[True, False, False])
            st.success(f"老周，數據已對齊！最新更新日期：{max_date}")
            st.dataframe(final_df.drop(columns=['sort_rank']), use_container_width=True, hide_index=True)
        else:
            st.error("查無資料，請確認 Google 中繼站(GAS) 是否有 4/27 以後的數據。")
