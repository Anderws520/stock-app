import streamlit as st
import pandas as pd
import requests
from datetime import datetime
import time

st.set_page_config(page_title="專業操盤系統-終極對齊版", layout="wide")
st.title("🛡️ 買點定位系統 (100% 數據對齊版)")

GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.sidebar.header("📅 交易時間軸設定")
start_date = st.sidebar.date_input("分析起始日期", datetime(2026, 4, 20))
end_date = st.sidebar.date_input("分析結束日期", datetime(2026, 4, 30))

def get_data_v2(date_str):
    clean_date = date_str.replace("-", "")
    try:
        resp = requests.get(f"{GAS_URL}?date={clean_date}&t={time.time()}", timeout=20)
        if resp.status_code == 200:
            json_data = resp.json()
            if json_data.get('stat') == 'OK' and json_data.get('data'):
                df = pd.DataFrame(json_data['data'], columns=json_data['fields'])
                # 強制清理標籤：移除空白與特殊字元
                df.columns = [str(c).strip().replace('\n','') for c in df.columns]
                df['日期'] = date_str
                return df
    except: pass
    return None

if st.button("🚀 執行「獲利潛力」專業掃描"):
    date_list = pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d").tolist()
    all_raw = []
    
    with st.spinner("正在執行全自動欄位對齊..."):
        p_bar = st.progress(0)
        for i, d in enumerate(date_list):
            df = get_data_v2(d)
            if df is not None: all_raw.append(df)
            p_bar.progress((i + 1) / len(date_list))
            time.sleep(0.1)

    if all_raw:
        full_df = pd.concat(all_raw)
        
        # --- 核心修正：解決 image_e30824.png 的格式不完整問題 ---
        def find_best_col(cols, keywords):
            for k in keywords:
                res = [c for c in cols if k in c]
                if res: return res[0]
            return None

        # 模糊搜尋必要欄位
        p_col = find_best_col(full_df.columns, ['收盤', '價格', '成交', '結算', 'Price', 'Last'])
        id_col = find_best_col(full_df.columns, ['代號', 'Code', 'Symbol'])
        name_col = find_best_col(full_df.columns, ['名稱', 'Name'])
        f_col = find_best_col(full_df.columns, ['外資', '外陸資'])
        i_col = find_best_col(full_df.columns, ['投信', 'ITC'])

        results = []
        for sid, group in full_df.groupby(id_col if id_col else full_df.columns[0]):
            group = group.sort_values('日期')
            
            # 數值強制清理 (處理逗號與空值)
            def clean_num(val):
                return pd.to_numeric(str(val).replace(',',''), errors='coerce') or 0

            prices = [clean_num(p) for p in group[p_col]] if p_col else [0]
            curr_p = prices[-1]
            ma5 = sum(prices[-5:]) / len(prices[-5:]) if prices else 0
            
            last_row = group.iloc[-1]
            # 計算法人買超
            total_buy = (clean_num(last_row.get(f_col, 0)) + clean_num(last_row.get(i_col, 0))) / 1000
            
            # 專業篩選建議
            count = len(group)
            if total_buy > 800 and count <= 2:
                advice, rank = "💎 雙強初現(首選)", 1
            elif count >= 3 and total_buy > 0:
                advice, rank = "🔥 資金鎖碼(續強)", 2
            else:
                advice, rank = "✅ 趨勢跟蹤", 3
            
            results.append({
                '日期': last_row['日期'],
                '股票代號': sid,
                '股票名稱': last_row[name_col] if name_col else "N/A",
                '買超張數': int(total_buy),
                '目前現價': round(curr_p, 2),
                '5日均價': round(ma5, 2),
                '價差%': f"{((curr_p-ma5)/ma5):.2%}" if ma5 != 0 else "0.00%",
                '連續天數': count,
                '操盤建議': advice,
                'priority': rank
            })
        
        final_df = pd.DataFrame(results).sort_values(['priority', '買超張數'], ascending=[True, False]).head(20)
        st.success(f"✅ 數據對齊成功！已掃描 {len(all_raw)} 個交易日。")
        st.dataframe(final_df.drop(columns=['priority']), use_container_width=True, hide_index=True)
    else:
        st.error("❌ 無法取得數據。可能是 GAS API 連結已失效或當日無交易。")
