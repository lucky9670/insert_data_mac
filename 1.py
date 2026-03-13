from pathlib import Path
import json

folder_path = Path(r"clay_company_next/")

files = [file.name for file in folder_path.iterdir() if file.is_file()]

with open("files.json", "w") as json_file:
    json.dump(files, json_file)

print(files)