import pandas as pd

CSV_PATH = r'd:\1 Projects\UFC\data\processed\fight_details.csv'

df = pd.read_csv(CSV_PATH)
print(df.dtypes)