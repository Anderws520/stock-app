import streamlit as st
import pandas as pd
import requests
from datetime import datetime
import time
import json

st.set_page_config(page_title="專業操盤系統-終極穩定版", layout="wide")
st.title("🛡️ 買點定位系統 (完全自力救濟版)")

# 這是目前看來不穩定的源頭，我們加上強力的 try-except 診斷
GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.sidebar.header("📅 交易時間軸設定")
start_date = st.sidebar.date_input("分析起始日期", datetime(2026, 4, 20))
end_date = st.sidebar.date_input("分析結束日期", datetime(2026, 4, 30))

def diagnostic_fetch(date_str):
    """
    最強數據診斷邏輯：確保知道每一秒發生什麼事
    """
    clean_date = date_str.replace("-", "")
    full_url = f"{GAS_URL}?date={clean_date}&t={time.time()}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        resp = requests.get(full_url, headers=headers, timeout=15)
        
        # 診斷 1: HTTP 狀態碼
        if resp.status_code != 200:
            return None, f"HTTP 錯誤: {resp.status_code}"
            
        # 診斷 2: 內容是否為空
        if not resp.text.strip():
            return None, "API 回傳空白內容"
            
        # 診斷 3: 是否為有效的 JSON
        try:
            json_data = resp.json()
        except Exception as e:
            return None, f"JSON 解析失敗: {str(e)} | 內容開頭: {resp.text[:50]}"

        # 診斷 4: 內部 stat 狀態
        if json_data.get('stat') != 'OK':
            return None, f"API 業務邏輯錯誤: {json_data.get('stat')}"
            
        # 診斷 5: 欄位與數據匹配
        data = json_data.get('data', [])
        fields = json_data.get('fields', [])
        if not data or not fields:
            return None, "數據庫目前無該日成交紀錄 (Data/Fields Empty)"
            
        df = pd.DataFrame(data, columns=fields)
        df.columns = [str(c).strip() for c in df.columns]
        df['日期'] = date_str
        return df, "Success"
        
    except requests.exceptions.Timeout:
        return None, "連線超時 (Timeout)"
    except Exception as e:
        return None, f"系統異常: {str(e)}"

if st.button("🚀 執行「獲利潛力」專業掃描"):
    date_list = pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d").tolist()
    all_raw_data = []
    logs = []
    
    with st.spinner("🔍 正透過多重管道校準數據中..."):
        p_bar = st.progress(0)
        for i, d in enumerate(date_list):
            df, status = diagnostic_fetch(d)
            if df is not None:
                all_raw_data.append(df)
            else:
                logs.append(f"{d}: {status}")
            p_bar.progress((i + 1) / len(date_list))
            time.sleep(0.1)

    if all_raw_data:
        full_df = pd.concat(all_raw_data)
        
        # 自動識別核心欄位
        p_col = next((c for c in full_df.columns if any(x in c for x in ['收盤', '價格', '成交', '價'])), None)
        id_col = next((c for c in full_df.columns if '代號' in c), None)
        name_col = next((c for c in full_df.columns if '名稱' in c), None)
        
        results = []
        for sid, group in full_df.groupby(id_col if id_col else full_df.columns[0]):
            group = group.sort_values('日期')
            
            def safe_num(v):
                return pd.to_numeric(str(v).replace(',',''), errors='coerce') or 0
                
            prices = [safe_num(p) for p in group[p_col]] if p_col else [0]
            curr_p = prices[-1]
            ma5 = sum(prices[-5:]) / len(prices[-5:]) if prices else 0
            
            last_row = group.iloc[-1]
            f_buy = safe_num(last_row.get('外陸資買賣超股數(不含外資自營商)', 0)) / 1000
            i_buy = safe_num(last_row.get('投信買賣超股數', 0)) / 1000
            total_buy = round(f_buy + i_buy, 0)
            
            advice, rank = ("💎 雙強初現(首選)", 1) if total_buy > 500 and len(group) <= 2 else (("🔥 趨勢續強", 2) if len(group) >= 3 else ("✅ 趨勢跟蹤", 3))
            
            results.append({
                '日期': last_row['日期'],
                '股票代號': sid,
                '股票名稱': last_row[name_col] if name_col else "N/A",
                '買超張數': int(total_buy),
                '目前現價': round(curr_p, 2),
                '5日均價': round(ma5, 2),
                '操盤建議': advice,
                'priority': rank
            })
            
        final_df = pd.DataFrame(results).sort_values(['priority', '買超張數'], ascending=[True, False]).head(20)
        st.success(f"✅ 成功對齊 {len(all_raw_data)} 天數據！")
        st.dataframe(final_df.drop(columns=['priority']), use_container_width=True, hide_index=True)
    else:
        st.error("❌ 數據全面斷聯。這代表該 API 服務器已關閉或無回應。")
        with st.expander("🛠️ 深度錯誤診斷報告 (請截圖這部分給我)"):
            for log in logs:
                st.write(log)
