import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import GetPriceModule

mk = GetPriceModule.MarketDB()
stocks = ['삼성전자', '셀트리온', '현대자동차', '카카오', 'GKL', '삼성중공업']
df = pd.DataFrame()
for s in stocks :
    df[s] = mk.get_daily_price(s, '2016-01-01', '2021-02-10')['close']

daily_ret = df.pct_change() 
annual_ret = daily_ret.mean() * 252
daily_cov = daily_ret.cov() 
annual_cov = daily_cov * 252

port_ret = [] 
port_risk = [] 
port_weights = []
sharpe_ratio = [] 

for _ in range(20000):
    weights = np.random.random(len(stocks)) 
    weights /= np.sum(weights) 

    returns = np.dot(weights, annual_ret) 
    risk = np.sqrt(np.dot(weights.T, np.dot(annual_cov, weights))) 

    port_ret.append(returns) 
    port_risk.append(risk) 
    port_weights.append(weights)
    sharpe_ratio.append(returns/risk) 
    
portfolio = {'Returns': port_ret, 'Risk': port_risk, 'Sharpe': sharpe_ratio}
for i, s in enumerate(stocks): 
    portfolio[s] = [weight[i] for weight in port_weights] 
df = pd.DataFrame(portfolio) 
df = df[['Returns', 'Risk', 'Sharpe'] + [s for s in stocks]]  

max_sharpe = df.loc[df['Sharpe'] == df['Sharpe'].max()] 
min_risk = df.loc[df['Risk'] == df['Risk'].min()]  

print("MAX SHARPE ::")
print(f"Returns = {max_sharpe['Returns'].values * 100}%, Risk = {max_sharpe['Risk'].values* 100}%")
for s in stocks:
    print(f"{s} Rate = {max_sharpe[s].values * 100}%")
print("MIN RISK ::")
print(f"Returns = {min_risk['Returns'].values * 100}, Risk = {min_risk['Risk'].values * 100}")
for s in stocks:
    print(f"{s} Rate = {min_risk[s].values * 100}")

# print(df)

df.plot.scatter(x = 'Risk', y = 'Returns', c = 'Sharpe', cmap = 'viridis', edgecolors='k', figsize=(11,7), grid=True)
plt.scatter(x=max_sharpe['Risk'], y = max_sharpe['Returns'], c = 'r', marker='*', s=300)
plt.scatter(x=min_risk['Risk'], y = min_risk['Returns'], c = 'r', marker='X', s=200)
plt.title('Portfolio Optimization')
plt.xlabel('Risk')
plt.ylabel('Expected Returns')
plt.show()
