path = r"C:\Users\halek\OneDrive\Desktop\Vision Board\SlateIQ\SlateIQ\NBA\step1_fetch_prizepicks_api.py"

with open(path, 'r', encoding='utf-8') as f:
    s = f.read()

# Check what's currently in the file around the get call
idx = s.find("r = session.get")
if idx >= 0:
    print("Found session.get at char", idx)
    print(repr(s[idx-50:idx+100]))
else:
    print("session.get NOT found")
    idx2 = s.find("r = requests.get")
    print("requests.get found:", idx2)
    if idx2 >= 0:
        print(repr(s[idx2-50:idx2+150]))
