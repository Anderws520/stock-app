import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import time

st.set_page_config(page_title="老周操盤手完全體", layout="wide")
st.title("🛡️ 買點定位系統 3.0 - 價格/籌碼全方位監控")

GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.sidebar.header("📅 戰略分析區區間")
start_date = st.sidebar.date_input("開始日期", datetime(2026, 4, 20))
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
                
                # 數值清理
                for col in ['外陸資買賣超股數(不含外資自營商)', '投信買賣超股數', '自營商買賣超股數', '收盤價']:
                    if col in df.columns:
                        df[col] = df[col].astype(str).str.replace(',','').replace('', '0').astype(float)
                
                df['外資張'] = (df['外陸資買賣超股數(不含外資自營商)'] / 1000).round(0)
                df['投信張'] = (df['投信買賣超股數'] / 1000).round(0)
                df['合計買超'] = df['外資張'] + df['投信張'] + (df['自營商買賣超股數']/1000).round(0)
                
                # 篩選個股
                is_stock = ~df['證券代號'].str.startswith('00')
                return df[is_stock & (df['投信張'] > 0) & (df['合計買超'] > 100)]
    except: return None
    return None

if st.button("🚀 執行全方位掃描"):
    date_range = pd.date_range(start=start_date, end=end_date).strftime("%Y%m%d").tolist()
    all_raw_data = []
    
    with st.spinner("正在校準籌碼與價格位階..."):
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
                
                # 計算 5 日均價 (以現有資料估算)
                avg_5d = group['收盤價'].tail(5).mean() if '收盤價' in group.columns else 0
                current_price = group.iloc[-1]['收盤價'] if '收盤價' in group.columns else 0
                price_diff = ((current_price - avg_5d) / avg_5d * 100) if avg_5d != 0 else 0
                
                # 標籤邏輯
                strong_days = group[(group['外資張'] > 0) & (group['投信張'] > 0)]
                first_date = strong_days.iloc[0]['日期'] if not strong_days.empty else group.iloc[0]['日期']
                
                is_first = len(group) <= 2
                advice = "💎 雙強初現(首選)" if is_first else "✅ 穩定增溫"
                
                last_info = group.iloc[-1]
                if last_info['日期'] == max_date:
                    results.append({
                        '代號': stock_id,
                        '名稱': last_info['證券名稱'],
                        '操盤建議': advice,
                        '5日均價': round(avg_5d, 2),
                        '目前現價': round(current_price, 2),
                        '價差%': f"{price_diff:.2f}%",
                        '今日合計': int(last_info['合計買超']),
                        '投信張': int(last_info['投信張']),
                        '連榜天數': len(group),
                        '最佳購買日期': f"{first_date[:4]}/{first_date[4:6]}/{first_date[6:]}",
                        'sort_rank': 0 if advice == "💎 雙強初現(首選)" else 1
                    })
            
            final_df = pd.DataFrame(results).sort_values(by=['sort_rank', '連榜天數'], ascending=[True, False])
            
            st.success(f"老周，位階監控已加入！目前最新行情：{max_date}")
            st.dataframe(final_df.drop(columns=['sort_rank']), use_container_width=True, hide_index=True)
