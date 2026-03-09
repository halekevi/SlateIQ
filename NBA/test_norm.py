import unicodedata

def norm(name):
    nfkd = unicodedata.normalize('NFKD', name)
    clean = ''.join([c for c in nfkd if not unicodedata.combining(c)])
    return clean.strip()

print("Luka Dončić ->", norm("Luka Dončić"))
print("Luka Doncic ->", norm("Luka Doncic"))
print()
print("Normalized to lowercase:")
print("Luka Dončić ->", norm("Luka Dončić").lower())
print("Luka Doncic ->", norm("Luka Doncic").lower())

