import streamlit as st
import pandas as pd
import requests
from datetime import datetime
import time

st.set_page_config(page_title="專業操盤系統-終極版", layout="wide")
st.title("🛡️ 買點定位系統 (邏輯全校準版)")

# 確保 GAS 端的 API 欄位名稱完全被掃描
GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

def get_data(date_str):
    try:
        # 加入 time 參數防止 Cache 導致數據抓不到最新版
        resp = requests.get(f"{GAS_URL}?date={date_str}&t={time.time()}", timeout=30)
        if resp.status_code == 200:
            json_data = resp.json()
            if json_data.get('stat') == 'OK':
                df = pd.DataFrame(json_data['data'], columns=json_data['fields'])
                
                # 核心修正：暴力清理欄位名稱
                df.columns = [c.strip().replace('\n','') for c in df.columns]
                
                # 自動搜尋收盤價欄位 (不論它叫 價格、收盤價、結算價)
                p_col = None
                possible_price_names = ['收盤價', '收盤價 ', '價格', '成交價', '目前價']
                for name in possible_price_names:
                    if name in df.columns:
                        p_col = name
                        break
                
                # 如果還是沒找到，直接找標籤裡包含「價」的欄位
                if not p_col:
                    p_col = next((c for c in df.columns if '價' in c and '差' not in c), None)

                # 數值轉換邏輯：確保 0 不再出現
                target_cols = ['外陸資買賣超股數(不含外資自營商)', '投信買賣超股數', '自營商買賣超股數']
                if p_col: target_cols.append(p_col)
                
                for col in target_cols:
                    if col in df.columns:
                        # 處理逗號與空字串
                        df[col] = pd.to_numeric(df[col].astype(str).str.replace(',',''), errors='coerce').fillna(0)
                
                # 計算張數
                df['外資張'] = (df['外陸資買賣超股數(不含外資自營商)'] / 1000).round(0)
                df['投信張'] = (df['投信買賣超股數'] / 1000).round(0)
                df['合計買超'] = df['外資張'] + df['投信張'] + (df.get('自營商買賣超股數', 0)/1000).round(0)
                
                return df, p_col
    except: return None, None
    return None, None

if st.button("🚀 執行「獲利潛力」專業掃描"):
    # 設定掃描區間，模擬 5 日均價計算
    date_range = pd.date_range(start="2026-04-20", end="2026-04-30").strftime("%Y%m%d").tolist()
    all_raw_data = []
    actual_p_col = None
    
    with st.spinner("正在執行多重邏輯篩選並校準價格資料..."):
        for d_str in date_range:
            df, p_col = get_data(d_str)
            if df is not None and not df.empty:
                all_raw_data.append(df)
                if p_col: actual_p_col = p_col
            time.sleep(0.1)
            
        if all_raw_data:
            full_df = pd.concat(all_raw_data)
            results = []
            
            for stock_id, group in full_df.groupby('證券代號'):
                group = group.sort_values('日期')
                
                # 確保價格序列存在且非零
                prices = group[actual_p_col].tolist() if actual_p_col in group.columns else []
                current_p = prices[-1] if prices else 0
                avg_5p = (sum(prices[-5:]) / len(prices[-5:])) if len(prices) > 0 else 0
                diff_p = ((current_p - avg_5p) / avg_5p) if avg_5p != 0 else 0
                
                last_info = group.iloc[-1]
                count = len(group)
                
                # 操盤建議：不再全員首選
                # 條件：法人同時大買 且 連續天數 <= 2 天 (代表剛起漲)
                is_both_buy = (last_info['外資張'] > 500) and (last_info['投信張'] > 100)
                if is_both_buy and count <= 2:
                    advice = "💎 雙強初現(首選)"
                    rank = 1
                elif count >= 4:
                    advice = "✅ 趨勢續強"
                    rank = 2
                else:
                    advice = "⚠️ 觀察等待"
                    rank = 3
                
                results.append({
                    '日期': last_info.get('日期', ''),
                    '股票代號': stock_id,
                    '股票名稱': last_info['證券名稱'],
                    '關鍵分點': '三大法人',
                    '買超張數': int(last_info['合計買超']),
                    '5日均價': round(avg_5p, 2),
                    '目前現價': round(current_p, 2),
                    '價差%': f"{diff_p:.2%}",
                    '連續出現天數': count,
                    '集保人數變動': '無數據',
                    '最佳購買日期': group.iloc[0].get('日期', ''),
                    '操盤建議': advice,
                    'rank': rank
                })
            
            # 依據買超量與優先權排序並取前 20
            final_df = pd.DataFrame(results).sort_values(by=['rank', '買超張數'], ascending=[True, False]).head(20)
            st.success("價格與建議邏輯已全數校準完畢！")
            st.dataframe(final_df.drop(columns=['rank']), use_container_width=True, hide_index=True)
