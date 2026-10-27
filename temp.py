import pandas as pd
import numpy as np

# 1. Ek choti si sample DataFrame banate hain
data = {
    "item": ["Apple", "Banana", "Milk"],
    "quantity": ["Expired", "", None] # Pehli row me reason hai, baaki khali hain
}
df = pd.DataFrame(data)
df['quarantine_reason']=pd.NA

df["quantity"]=df["quantity"].astype('string').str.strip().str.lower()
df['quantity']=df["quantity"].mask(df["quantity"].eq(""))

mask=df['quantity'].isna()

for col in df.columns:
    df.loc[mask,'quarantine_reason']="order placed"
    
print(df)

#"D:\Aman\aman.code\dataengproject\retail_mgmt_aws\key.pem"