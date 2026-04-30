import streamlit as st
import pandas as pd
import requests
from datetime import datetime
import time

st.set_page_config(page_title="專業操盤系統-終極穩定版", layout="wide")
st.title("🛡️ 買點定位系統 (100% 穩定運作版)")

GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.sidebar.header("📅 交易時間軸")
start_date = st.sidebar.date_input("分析起始日期", datetime(2026, 4, 20))
end_date = st.sidebar.date_input("分析結束日期", datetime(2026, 4, 30))

def get_data(date_str):
    try:
        # 移除橫槓，確保格式為 YYYYMMDD
        clean_date = date_str.replace("-", "")
        # 加入 time.time() 強制跳過瀏覽器快取
        api_link = f"{GAS_URL}?date={clean_date}&t={time.time()}"
        
        resp = requests.get(api_link, timeout=15)
        if resp.status_code == 200:
            json_data = resp.json()
            # 檢查 stat 且 data 不能是空的
            if json_data.get('stat') == 'OK' and len(json_data.get('data', [])) > 0:
                df = pd.DataFrame(json_data['data'], columns=json_data['fields'])
                df.columns = [c.strip() for c in df.columns]
                df['日期'] = date_str
                return df, "Success"
            return None, f"無資料 (Stat: {json_data.get('stat')})"
        return None, f"HTTP 錯誤: {resp.status_code}"
    except Exception as e:
        return None, f"異常: {str(e)}"

if st.button("🚀 執行「獲利潛力」前 20 強篩選"):
    # 生成 YYYY-MM-DD 格式清單
    date_list = pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d").tolist()
    all_raw_data = []
    debug_info = []
    
    with st.spinner("正在掃描數據中心..."):
        progress_bar = st.progress(0)
        for i, d_str in enumerate(date_list):
            df, status = get_data(d_str)
            if df is not None:
                all_raw_data.append(df)
            else:
                debug_info.append(f"{d_str}: {status}")
            progress_bar.progress((i + 1) / len(date_list))
            time.sleep(0.05)
            
        if all_raw_data:
            full_df = pd.concat(all_raw_data)
            
            # 自動找價格、代號、名稱
            p_col = next((c for c in full_df.columns if any(x in c for x in ['收盤', '價格', '成交'])), None)
            id_col = next((c for c in full_df.columns if '代號' in c), '證券代號')
            name_col = next((c for c in full_df.columns if '名稱' in c), '證券名稱')
            
            results = []
            for stock_id, group in full_df.groupby(id_col):
                group = group.sort_values('日期')
                
                # 處理價格
                prices = pd.to_numeric(group[p_col].astype(str).str.replace(',',''), errors='coerce').fillna(0).tolist() if p_col else [0]
                current_p = prices[-1]
                avg_5p = sum(prices[-5:]) / len(prices[-5:]) if len(prices) > 0 else 0
                diff_p = ((current_p - avg_5p) / avg_5p) if avg_5p != 0 else 0
                
                last_row = group.iloc[-1]
                # 法人買超張數計算
                f_buy = pd.to_numeric(str(last_row.get('外陸資買賣超股數(不含外資自營商)', 0)).replace(',',''), errors='coerce') / 1000
                i_buy = pd.to_numeric(str(last_row.get('投信買賣超股數', 0)).replace(',',''), errors='coerce') / 1000
                total_buy = round(f_buy + i_buy, 0)
                
                # 篩選建議
                count = len(group)
                if total_buy > 200 and count <= 2:
                    advice, rank = "💎 雙強初現(首選)", 1
                elif count >= 3 and total_buy > 0:
                    advice, rank = "🔥 趨勢續強", 2
                else:
                    advice, rank = "✅ 趨勢跟蹤", 3
                
                results.append({
                    '日期': last_row['日期'],
                    '股票代號': stock_id,
                    '股票名稱': last_row[name_col],
                    '買超張數': int(total_buy),
                    '5日均價': round(avg_5p, 2),
                    '目前現價': round(current_p, 2),
                    '價差%': f"{diff_p:.2%}",
                    '連續出現天數': count,
                    '操盤建議': advice,
                    'priority': rank
                })
            
            final_df = pd.DataFrame(results).sort_values(['priority', '買超張數'], ascending=[True, False]).head(20)
            st.success(f"✅ 成功抓取 {len(all_raw_data)} 天數據！")
            st.dataframe(final_df.drop(columns=['priority']), use_container_width=True, hide_index=True)
            
        else:
            st.error("❌ 期間內無任何有效交易數據。")
            with st.expander("查看數據掃描日誌 (Debug)"):
                for info in debug_info:
                    st.write(info)
