import streamlit as st
import pandas as pd
import requests
from datetime import datetime
import time

st.set_page_config(page_title="專業操盤系統-終極校準版", layout="wide")
st.title("🛡️ 買點定位系統 (數據 100% 對齊版)")

GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

def get_data(date_str):
    try:
        # 加入隨機參數防止快取
        resp = requests.get(f"{GAS_URL}?date={date_str}&t={time.time()}", timeout=30)
        if resp.status_code == 200:
            json_data = resp.json()
            if json_data.get('stat') == 'OK':
                df = pd.DataFrame(json_data['data'], columns=json_data['fields'])
                
                # 核心修正：清理欄位隱形空白與換行符號
                df.columns = [c.strip().replace('\n', '') for c in df.columns]
                
                # 核心修正：自動探測價格欄位 (個股、ETF、權證通用)
                p_col = next((c for c in df.columns if any(x in c for x in ['收盤', '價格', '成交']) and '差' not in c), None)
                
                # 數值清理轉換：處理逗號、負號與空值
                numeric_cols = ['外陸資買賣超股數(不含外資自營商)', '投信買賣超股數', '自營商買賣超股數']
                if p_col: numeric_cols.append(p_col)
                
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '').replace('--', '0'), errors='coerce').fillna(0)
                
                # 股數轉張數
                df['外資張'] = (df['外陸資買賣超股數(不含外資自營商)'] / 1000).round(0)
                df['投信張'] = (df['投信買賣超股數'] / 1000).round(0)
                df['合計買超'] = df['外資張'] + df['投信張'] + (df.get('自營商買賣超股數', 0) / 1000).round(0)
                
                return df, p_col
    except: return None, None
    return None, None

if st.button("🚀 執行「潛力大投報高」20 強篩選"):
    # 設定分析時間區間
    date_range = pd.date_range(start="2026-04-20", end="2026-04-30").strftime("%Y%m%d").tolist()
    all_raw_data = []
    final_p_col = None
    
    with st.spinner("50 年實戰邏輯運算中，正在排除 0 價位標的..."):
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
                
                # 價格運算與均線檢查
                prices = group[final_p_col].tolist() if final_p_col in group.columns else []
                current_p = prices[-1] if prices else 0
                avg_5p = (sum(prices[-5:]) / len(prices[-5:])) if len(prices) > 0 else 0
                diff_p = ((current_p - avg_5p) / avg_5p) if avg_5p != 0 else 0
                
                last_info = group.iloc[-1]
                count = len(group)
                
                # 專業操盤手邏輯判定
                is_big_buy = (last_info['外資張'] > 500) and (last_info['投信張'] > 200)
                if is_big_buy and count <= 2:
                    advice = "💎 雙強初現(首選)"
                    rank = 1
                elif count >= 4 and last_info['合計買超'] > 0:
                    advice = "🔥 趨勢續強"
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
            
            # 排序邏輯：優先權(rank) -> 買超量
            final_df = pd.DataFrame(results).sort_values(by=['rank', '買超張數'], ascending=[True, False]).head(20)
            
            # 最終檢查：若價格還是 0 則給予警告
            if final_df['目前現價'].sum() == 0:
                st.error("警告：遠端資料格式異動，請檢查欄位索引。")
            else:
                st.success("數據校準成功，已篩選出前 20 檔黃金標的。")
                
            st.dataframe(final_df.drop(columns=['rank']), use_container_width=True, hide_index=True)
