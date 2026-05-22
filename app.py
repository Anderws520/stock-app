if not resp.text or "查詢無資料" in resp.text or "HTML" in resp.text or "證券代號" not in resp.text:
    return "SKIPPED"
