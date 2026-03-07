path = r"C:\Users\halek\OneDrive\Desktop\Vision Board\SlateIQ\SlateIQ\NBA\step1_fetch_prizepicks_api.py"

with open(path, 'r', encoding='utf-8') as f:
    s = f.read()

# Replace the entire _api_get body with a simple stateless fetch
old_fn = '''def _api_get(
    session: requests.Session,
    url: str,
    params: dict,
    retries: int = 5,
    timeout: Tuple[float, float] = (10.0, 30.0),
) -> dict:
    # NOTE: intentionally ignores session — fresh request per call avoids 403 fingerprinting'''

new_fn = '''def _api_get(
    session: requests.Session,
    url: str,
    params: dict,
    retries: int = 5,
    timeout: Tuple[float, float] = (10.0, 30.0),
) -> dict:
    """Stateless GET — builds URL manually to avoid params-encoding 403 issues."""
    import urllib.parse
    qs = urllib.parse.urlencode(params)
    full_url = f"{url}?{qs}"
    WORKING_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept":     "application/json, text/plain, */*",
        "Referer":    "https://app.prizepicks.com/",
        "Origin":     "https://app.prizepicks.com",
    }'''

if old_fn in s:
    s = s.replace(old_fn, new_fn)
    print("Replaced _api_get header")
else:
    print("Could not find _api_get header — check manually")

# Replace the actual request line
old_req = '            ua = random.choice(USER_AGENTS)\n            hdrs = {**BASE_HEADERS, "User-Agent": ua}\n            r = requests.get(url, params=params, headers=hdrs, timeout=timeout)'
new_req = '            r = requests.get(full_url, headers=WORKING_HEADERS, timeout=timeout)'

if old_req in s:
    s = s.replace(old_req, new_req)
    print("Replaced request line")
else:
    print("Could not find request line")

with open(path, 'w', encoding='utf-8') as f:
    f.write(s)
print("Done")
