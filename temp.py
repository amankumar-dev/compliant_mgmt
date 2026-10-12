import pandas as pd
import numpy as np

# 1. Ek choti si sample DataFrame banate hain
data = {
    "item": ["Apple", "Banana", "Milk"],
    "quarantine_reason": ["Expired", "", None] # Pehli row me reason hai, baaki khali hain
}
df = pd.DataFrame(data)

print("--- Original DataFrame ---")
print(df)
print("\n" + "="*40 + "\n")

# Wahi function jo aapne pucha tha
def add_reason(df, mask, reason):
    """Append a quarantine reason without losing existing reasons."""
    if mask.any():
        existing = df.loc[mask, "quarantine_reason"].fillna("")
        df.loc[mask, "quarantine_reason"] = np.where(
            existing.eq(""),
            reason,
            existing + ";" + reason,
        )

# 2. Mask banate hain: Hum chahte hain ki row index 1 aur 2 par naya reason jude
mask = df.index.isin([1, 2])

# 3. Function ko call karte hain naya reason dene ke liye
add_reason(df, mask, "Damaged Box")

print("--- Reason Add Hone ke Baad ka DataFrame ---")
print(df)
print(mask)
