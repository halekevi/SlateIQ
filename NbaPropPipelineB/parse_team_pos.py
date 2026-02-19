import re
import pandas as pd

# Regular expression to match the player team and position format
TEAM_POS_RE = re.compile(r"^\s*([A-Z]{3})\s*-\s*([A-Z]{1,2}(?:-[A-Z]{1,2})?)\s*$")

# Sample raw data
raw_data = [
    "SAS - F-C",
    "CHA - F",
    "SAS - G",
    "CHA - G-F",
]

# Function to parse the team and position
def parse_team_pos(lines):
    parsed_rows = []
    
    for line in lines:
        match = TEAM_POS_RE.match(line.strip())
        if match:
            team = match.group(1)  # First 3 letters for team abbreviation
            pos = match.group(2)   # Player position (e.g., F-C, G, G-F)
            parsed_rows.append({"team": team, "pos": pos})
    
    return parsed_rows

# Run the parser on raw data
parsed_data = parse_team_pos(raw_data)

# Convert the parsed data to a DataFrame for easy visualization
df = pd.DataFrame(parsed_data)

# Display the resulting DataFrame
print(df)
