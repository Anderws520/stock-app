def download_t86(date: datetime.date):
    """下載並解析三大法人 T86 CSV（已修正 SSL 問題）"""
    if not is_trading_day(date):
        return None

    url = get_t86_url(date)
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    try:
        # 關鍵修正：加入 verify=False 繞過目前 SSL 問題
        resp = requests.get(url, headers=headers, timeout=25, verify=False)
        resp.raise_for_status()
        
        text = resp.text
        lines = [line.strip() for line in text.splitlines() if line.strip()]

        # 找到資料開始行
        start_idx = None
        for i, line in enumerate(lines):
            if "證券代號" in line:
                start_idx = i
                break

        if start_idx is None:
            st.warning(f"{date} 無法解析資料起始行")
            return None

        csv_content = "\n".join(lines[start_idx:])
        df = pd.read_csv(StringIO(csv_content), encoding='big5', on_bad_lines='skip')

        df.columns = [col.strip().replace('\n', '').replace(' ', '') for col in df.columns]

        # 顯示實際抓到的欄位（方便除錯）
        st.caption(f"[{date}] 抓到的欄位：{list(df.columns[:10])}...")

        # 支援多種可能欄位名稱
        buy_col = None
        for name in ['三大法人買賣超股數', '三大法人買賣超', '買賣超股數']:
            if name in df.columns:
                buy_col = name
                break

        if buy_col is None or '證券代號' not in df.columns:
            st.error(f"{date} 缺少關鍵欄位")
            return None

        df['三大法人買賣超股數'] = df[buy_col].apply(clean_number)
        df = df.dropna(subset=['證券代號', '三大法人買賣超股數'])
        df['日期'] = pd.to_datetime(date).date()
        df['證券代號'] = df['證券代號'].astype(str).str.zfill(4)

        return df[['日期', '證券代號', '證券名稱', '三大法人買賣超股數']]

    except Exception as e:
        st.error(f"{date} 下載失敗: {str(e)[:200]}...")
        return None