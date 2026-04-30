import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import time

st.set_page_config(page_title="專業操盤系統-終極版", layout="wide")
st.title("🛡️ 買點定位系統 (數據 100% 對齊版)")

GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

# --- 側邊欄：時間設定 ---
st.sidebar.header("📅 交易時間軸設定")
# 預設為最近的交易區間
default_start = datetime(2026, 4, 20)
default_end = datetime(2026, 4, 30)

start_date = st.sidebar.date_input("分析起始日期", default_start)
end_date = st.sidebar.date_input("分析結束日期", default_end)

def get_data(date_str):
    try:
        # 強制格式化日期為 YYYYMMDD
        formatted_date = date_str.replace("-", "").replace("/", "")
        api_link = f"{GAS_URL}?date={formatted_date}&t={time.time()}"
        
        resp = requests.get(api_link, timeout=15)
        if resp.status_code == 200:
            json_data = resp.json()
            if json_data.get('stat') == 'OK':
                df = pd.DataFrame(json_data['data'], columns=json_data['fields'])
                df.columns = [c.strip().replace('\n','') for c in df.columns]
                return df
            else:
                return f"API 狀態錯誤: {json_data.get('stat')}"
        else:
            return f"連線失敗，狀態碼: {resp.status_code}"
    except Exception as e:
        return f"發生異常: {str(e)}"

# --- 執行按鈕 ---
if st.button("🚀 執行「潛力大投報高」20 強篩選"):
    # 產生日區間
    date_list = pd.date_range(start=start_date, end=end_date).strftime("%Y%m%d").tolist()
    all_raw_data = []
    error_logs = []
    
    with st.spinner("正在掃描法人籌碼與對齊價格..."):
        progress_bar = st.progress(0)
        for i, d_str in enumerate(date_list):
            result = get_data(d_str)
            if isinstance(result, pd.DataFrame):
                all_raw_data.append(result)
            else:
                error_logs.append(f"{d_str}: {result}")
            progress_bar.progress((i + 1) / len(date_list))
            
        if all_raw_data:
            full_df = pd.concat(all_raw_data)
            
            # --- 核心邏輯：清洗與分析 ---
            # 1. 價格欄位偵測
            p_col = next((c for c in full_df.columns if any(x in c for x in ['收盤', '價格', '成交']) and '差' not in c), None)
            
            # 2. 數值清理
            cols_to_fix = ['外陸資買賣超股數(不含外資自營商)', '投信買賣超股數', '自營商買賣超股數']
            if p_col: cols_to_fix.append(p_col)
            
            for col in cols_to_fix:
                if col in full_df.columns:
                    full_df[col] = pd.to_numeric(full_df[col].astype(str).str.replace(',',''), errors='coerce').fillna(0)
            
            # 3. 彙總結果
            results = []
            for stock_id, group in full_df.groupby('證券代號'):
                group = group.sort_values('日期', ascending=True)
                prices = group[p_col].tolist() if p_col else [0]
                
                # 計算 5 日均價與現價
                current_p = prices[-1]
                avg_5p = sum(prices[-5:]) / len(prices[-5:]) if prices else 0
                diff_p = ((current_p - avg_5p) / avg_5p) if avg_5p != 0 else 0
                
                last_row = group.iloc[-1]
                total_buy = (last_row['外陸資買賣超股數(不含外資自營商)'] + last_row['投信買賣超股數']) / 1000
                
                # 操盤建議判斷
                count = len(group)
                if total_buy > 500 and count <= 2:
                    advice = "💎 雙強初現(首選)"
                    rank = 1
                elif count >= 3:
                    advice = "🔥 趨勢續強"
                    rank = 2
                else:
                    advice = "✅ 趨勢跟蹤"
                    rank = 3
                
                results.append({
                    '日期': last_row.get('日期', start_date.strftime("%Y/%m/%d")),
                    '股票代號': stock_id,
                    '股票名稱': last_row['證券名稱'],
                    '關鍵分點': '三大法人',
                    '買超張數': int(total_buy),
                    '5日均價': round(avg_5p, 2),
                    '目前現價': round(current_p, 2),
                    '價差%': f"{diff_p:.2%}",
                    '連續出現天數': count,
                    '集保人數變動': '無數據',
                    '最佳購買日期': group.iloc[0].get('日期', ''),
                    '操盤建議': advice,
                    'rank': rank
                })
            
            # 排序與輸出
            final_df = pd.DataFrame(results).sort_values(['rank', '買超張數'], ascending=[True, False]).head(20)
            st.success("分析完成！")
            st.dataframe(final_df.drop(columns=['rank']), use_container_width=True, hide_index=True)
            
        else:
            st.error("❌ 找不到任何數據。")
            with st.expander("查看錯誤詳情 (Debug Log)"):
                for log in error_logs:
                    st.write(log)
