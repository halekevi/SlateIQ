import re

path = r"C:\Users\halek\OneDrive\Desktop\Vision Board\SlateIQ\SlateIQ\NBA\step1_fetch_prizepicks_api.py"

with open(path, 'r', encoding='utf-8') as f:
    s = f.read()

# Replace _api_get to use fresh requests.get instead of session
old = '''def _api_get(
    session: requests.Session,
    url: str,
    params: dict,
    retries: int = 5,
    timeout: Tuple[float, float] = (10.0, 30.0),
) -> dict:'''

new = '''def _api_get(
    session: requests.Session,
    url: str,
    params: dict,
    retries: int = 5,
    timeout: Tuple[float, float] = (10.0, 30.0),
) -> dict:
    # NOTE: intentionally ignores session — fresh request per call avoids 403 fingerprinting'''

s = s.replace(old, new)

# Replace session.get with requests.get inside _api_get
s = s.replace(
    '            r = session.get(url, params=params, timeout=timeout)',
    '            ua = random.choice(USER_AGENTS)\n            hdrs = {**BASE_HEADERS, "User-Agent": ua}\n            r = requests.get(url, params=params, headers=hdrs, timeout=timeout)'
)

with open(path, 'w', encoding='utf-8') as f:
    f.write(s)
print("Patched!")
