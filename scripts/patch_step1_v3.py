path = r"C:\Users\halek\OneDrive\Desktop\Vision Board\SlateIQ\SlateIQ\NBA\step1_fetch_prizepicks_api.py"

with open(path, 'r', encoding='utf-8') as f:
    s = f.read()

# Replace BASE_HEADERS to include Referer and Origin which are required
old = '''BASE_HEADERS = {
    "Accept":             "application/json, text/plain, */*",
    "Accept-Language":    "en-US,en;q=0.9",
    "Accept-Encoding":    "gzip, deflate, br",
    "Connection":         "keep-alive",
    "Referer":            "https://app.prizepicks.com/",
    "Origin":             "https://app.prizepicks.com",
    "Sec-Fetch-Dest":     "empty",
    "Sec-Fetch-Mode":     "cors",
    "Sec-Fetch-Site":     "same-site",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "DNT":                "1",
}'''

new = '''BASE_HEADERS = {
    "Accept":             "application/json, text/plain, */*",
    "Accept-Language":    "en-US,en;q=0.9",
    "Accept-Encoding":    "gzip, deflate, br",
    "Connection":         "keep-alive",
    "Referer":            "https://app.prizepicks.com/",
    "Origin":             "https://app.prizepicks.com",
    "Sec-Fetch-Dest":     "empty",
    "Sec-Fetch-Mode":     "cors",
    "Sec-Fetch-Site":     "same-site",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "DNT":                "1",
    # Required to avoid 403 — must be sent on every request
    "X-Requested-With":   "XMLHttpRequest",
}'''

if old in s:
    s = s.replace(old, new)
    print("Updated BASE_HEADERS")
else:
    print("BASE_HEADERS block not found exactly — showing current:")
    idx = s.find("BASE_HEADERS")
    print(repr(s[idx:idx+400]))

# Also remove the _make_session sleep delay which triggers rate limiting detection
s = s.replace(
    '    # Brief pause before first request — looks more human\n    time.sleep(random.uniform(0.5, 1.5))',
    '    # Session sleep removed — using stateless requests.get instead'
)

with open(path, 'w', encoding='utf-8') as f:
    f.write(s)
print("Done")
