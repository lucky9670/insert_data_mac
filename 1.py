from pathlib import Path

folder_path = Path(r"clay_company_100/")

files = [file.name for file in folder_path.iterdir() if file.is_file()]

print(files)