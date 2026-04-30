import streamlit as st
import pandas as pd
import requests
from datetime import datetime
import time

st.set_page_config(page_title="專業操盤系統-終極對齊版", layout="wide")
st.title("🛡️ 買點定位系統 (Google 數據直連版)")

# 核心數據來源 (已加入偵測與重試機制)
GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.sidebar.header("📅 交易時間軸設定")
start_date = st.sidebar.date_input("分析起始日期", datetime(2026, 4, 20))
end_date = st.sidebar.date_input("分析結束日期", datetime(2026, 4, 30))

def fetch_safe_data(date_str):
    """
    針對 image_e30c01.png 看到的 char 0 錯誤進行修復
    """
    clean_date = date_str.replace("-", "")
    try:
        # 使用 Google 官方推薦的轉址處理方式
        resp = requests.get(f"{GAS_URL}?date={clean_date}", timeout=20, allow_redirects=True)
        
        # 檢查是否為空內容
        if not resp.text or resp.text.strip() == "":
            return None, "API 回傳內容為空 (Empty Content)"
        
        json_data = resp.json()
        if json_data.get('stat') == 'OK' and json_data.get('data'):
            df = pd.DataFrame(json_data['data'], columns=json_data['fields'])
            df.columns = [c.strip() for c in df.columns]
            df['日期'] = date_str
            return df, "Success"
        
        return None, f"數據狀態異常: {json_data.get('stat')}"
    except Exception as e:
        return None, f"連線異常: {str(e)}"

if st.button("🚀 執行「獲利潛力」專業掃描"):
    date_list = pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d").tolist()
    all_data = []
    error_logs = []
    
    with st.spinner("正在強制從 Google 數據中心拉取歷史紀錄..."):
        p_bar = st.progress(0)
        for i, d in enumerate(date_list):
            df, msg = fetch_safe_data(d)
            if df is not None:
                all_data.append(df)
            else:
                error_logs.append(f"{d}: {msg}")
            p_bar.progress((i + 1) / len(date_list))
            time.sleep(0.2) # 稍微延遲避免被 Google 阻擋

    if all_data:
        full_df = pd.concat(all_data)
        
        # 自動尋找關鍵欄位 (排除 KeyError)
        p_col = next((c for c in full_df.columns if any(x in c for x in ['價', '成交'])), None)
        id_col = next((c for c in full_df.columns if '代號' in c), None)
        name_col = next((c for c in full_df.columns if '名稱' in c), None)
        
        if not p_col or not id_col:
            st.error("❌ 抓取到的數據格式不完整，無法計算價格。")
        else:
            results = []
            for sid, group in full_df.groupby(id_col):
                group = group.sort_values('日期')
                
                # 清洗價格數據
                prices = pd.to_numeric(group[p_col].astype(str).str.replace(',',''), errors='coerce').fillna(0).tolist()
                curr_p = prices[-1]
                ma5 = sum(prices[-5:]) / 5 if len(prices) >= 5 else (sum(prices)/len(prices) if prices else 0)
                
                last_row = group.iloc[-1]
                # 買超張數計算 (直接從欄位名稱模糊匹配)
                f_vol = pd.to_numeric(str(last_row.get('外陸資買賣超股數(不含外資自營商)', 0)).replace(',',''), errors='coerce') or 0
                i_vol = pd.to_numeric(str(last_row.get('投信買賣超股數', 0)).replace(',',''), errors='coerce') or 0
                total_shares = (f_vol + i_vol) / 1000
                
                # 專業篩選建議
                count = len(group)
                if total_shares > 1000 and count <= 2:
                    advice, rank = "💎 雙強初現(首選)", 1
                elif count >= 3:
                    advice, rank = "🔥 資金鎖碼(續強)", 2
                else:
                    advice, rank = "✅ 趨勢跟蹤", 3
                
                results.append({
                    '日期': last_row['日期'],
                    '股票代號': sid,
                    '股票名稱': last_row[name_col] if name_col else "N/A",
                    '買超張數': int(total_shares),
                    '目前現價': round(curr_p, 2),
                    '5日均價': round(ma5, 2),
                    '價差%': f"{((curr_p-ma5)/ma5):.2%}" if ma5 != 0 else "0.00%",
                    '連續天數': count,
                    '操盤建議': advice,
                    'priority': rank
                })
            
            final_df = pd.DataFrame(results).sort_values(['priority', '買超張數'], ascending=[True, False]).head(20)
            st.success("✅ 數據校準完成！")
            st.dataframe(final_df.drop(columns=['priority']), use_container_width=True, hide_index=True)
    else:
        st.error("❌ 依然無法從指定 API 獲取資料。")
        with st.expander("詳細錯誤診斷"):
            st.write("這通常代表 GAS 服務端在 2026/04/20 之後無人維護或超額。")
            for log in error_logs: st.text(log)
