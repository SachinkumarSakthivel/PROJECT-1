import pandas as pd
import numpy as np
import statsmodels.api as sm
from sklearn.linear_model import LinearRegression
from sklearn.metrics import accuracy_score,precision_score,recall_score,f1_score
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeRegressor
df=pd.read_csv('/content/ResaleFlatPricesBasedonApprovalDate19901999.csv')
dup_df=df[['month'	,'town',	'flat_type'	,'street_name'	,'storey_range','flat_model']]
x=df[['month'	,'town',	'flat_type'	,'block'	,'street_name'	,'storey_range','floor_area_sqm',	'flat_model',	'lease_commence_date']]
y=df['resale_price']
x_train,x_test,y_train,y_test=train_test_split(x,y,test_size=0.2,random_state=42)
m=DecisionTreeRegressor().fit(x_train,y_train)
m.predict([[0,6,1,27,4,3,2,2,10]])
r=sm.OLS(y,x)
res=r.fit()
res.summary()