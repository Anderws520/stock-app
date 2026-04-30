import streamlit as st
import pandas as pd
import requests
from datetime import datetime
import time

st.set_page_config(page_title="專業操盤手-終極篩選系統", layout="wide")
st.title("🛡️ 買點定位系統 (50年實戰邏輯版)")

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
                
                # 解決 image_e3ddfc.jpg 的價格問題：動態偵測收盤價欄位
                p_col = next((c for c in df.columns if any(x in c for x in ['收盤', '價格', '成交'])), None)
                
                # 數值清理邏輯 (關鍵：處理千分位並確保非 0)
                target_nums = ['外陸資買賣超股數(不含外資自營商)', '投信買賣超股數', '自營商買賣超股數']
                if p_col: target_nums.append(p_col)
                
                for col in target_nums:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col].astype(str).str.replace(',',''), errors='coerce').fillna(0)
                
                # 計算法人張數與力道
                df['外資張'] = (df['外陸資買賣超股數(不含外資自營商)'] / 1000).round(0)
                df['投信張'] = (df['投信買賣超股數'] / 1000).round(0)
                df['合計買超'] = df['外資張'] + df['投信張'] + (df.get('自營商買賣超股數', 0)/1000).round(0)
                
                return df, p_col
    except: return None, None
    return None, None

if st.button("🔍 執行「獲利潛力」前 20 強掃描"):
    date_range = pd.date_range(start=start_date, end=end_date).strftime("%Y%m%d").tolist()
    all_raw_data = []
    final_p_col = None
    
    with st.spinner("正在執行多重邏輯篩選..."):
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
                prices = group[final_p_col].tolist() if final_p_col in group.columns else []
                current_p = prices[-1] if prices else 0
                avg_5p = sum(prices[-5:]) / len(prices[-5:]) if len(prices) >= 5 else (sum(prices)/len(prices) if prices else 0)
                diff_p = ((current_p - avg_5p) / avg_5p) if avg_5p != 0 else 0
                
                # 專業篩選邏輯：
                last_row = group.iloc[-1]
                count = len(group)
                
                # 1. 判斷是否為「雙強初現」：最新一日外資與投信皆買超，且連續天數小於等於 2
                is_double_strong = (last_row['外資張'] > 0 and last_row['投信張'] > 0)
                
                # 2. 判斷是否有「法人鎖碼」：總買超金額佔比高 (此處以買超張數為權重)
                buy_strength = last_row['合計買超']
                
                # 3. 給予專業建議
                if is_double_strong and count <= 2:
                    advice = "💎 雙強初現(首選)"
                elif last_row['投信張'] > 500 and count > 3:
                    advice = "🔥 投信鎖碼(波段)"
                elif diff_p < 0 and buy_strength > 1000:
                    advice = "💰 底部佈局(低吸)"
                else:
                    advice = "✅ 趨勢跟蹤"
                
                results.append({
                    '日期': last_row['日期'],
                    '股票代號': stock_id,
                    '股票名稱': last_row['證券名稱'],
                    '關鍵分點': '三大法人',
                    '買超張數': int(buy_strength),
                    '5日均價': round(avg_5p, 2),
                    '目前現價': round(current_p, 2),
                    '價差%': f"{diff_p:.2%}",
                    '連續出現天數': count,
                    '集保人數變動': '無數據',
                    '最佳購買日期': group.iloc[0]['日期'],
                    '操盤建議': advice
                })
            
            # 最終排序：依據買超張數與操盤建議權重
            final_df = pd.DataFrame(results)
            # 賦予權重以利排序：首選最前，其次波段，其餘靠後
            advice_map = {"💎 雙強初現(首選)": 1, "🔥 投信鎖碼(波段)": 2, "💰 底部佈局(低吸)": 3, "✅ 趨勢跟蹤": 4}
            final_df['order'] = final_df['操盤建議'].map(advice_map)
            
            final_output = final_df.sort_values(by=['order', '買超張數'], ascending=[True, False]).head(20)
            st.dataframe(final_output.drop(columns=['order']), use_container_width=True, hide_index=True)
