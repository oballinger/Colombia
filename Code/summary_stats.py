import pandas as pd
import numpy as np
import matplotlib.pyplot as plt 

df=pd.read_csv('Data/Clean/CoC.csv')
plt.hist(df['size'])

plt.show()
print(df.columns)

