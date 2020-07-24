import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

#We read the csv file and initialize the pandas Dataframes
df = pd.read_csv("filtered_final_data.csv")
gf = pd.read_csv("high_quality_dataframe.csv")
df.columns = ['num', 'name', 'edits', 'editors', 'date', 'views']
gf.columns = ['num', 'name', 'high_quality_edits', 'editors', 'date', 'id', 'views']
gf = gf[['num', 'name', 'high_quality_edits', 'editors', 'date', 'views']]
df1 = df[['views', 'edits']]
gf1 = gf[['views', 'high_quality_edits']]


#graph of the log of number of edits
df1['groups'] = pd.cut(df1.views, np.arange(0, 100000, 10000))    #group the views
df2 = df1[['groups', 'edits']]
df2 = df2.groupby(['groups']).agg('mean').reset_index() #computing the average for the number of edits for each group
ax = df2.plot.line(x = 'groups', y = 'edits', logy = True)

#high quality articles
gf1['groups'] = pd.cut(gf1.views, np.arange(0, 100000, 10000))
gf2 = gf1[['groups', 'high_quality_edits']]
gf2 = gf2.groupby(['groups']).agg('mean').reset_index()
gf2.plot.line(ax = ax, x = 'groups', y = 'high_quality_edits', logy = True)


#normalized graph
def std_dev(x): return np.std(x)

df['year'] = pd.DatetimeIndex(df['date']).year
df['month'] = pd.DatetimeIndex(df['date']).month
df['log_edits'] = np.log(df['edits'])
df['age'] = 2021 - df['year']
df4 = df[['year', 'month', 'log_edits']]
df4 = df4.groupby(['year', 'month']).agg(['mean', std_dev]).reset_index() # we compute the average number of edits for each month and the standard deviation

df5 = pd.merge(df, df4, how='left', left_on = ['year', 'month'], right_on = ['year', 'month'])
df5.columns = ['num', 'name', 'edits', 'editors', 'date', 'views', 'years', 'month', 'log_edits', 'age', 'log_edits_mean', 'log_edits_std']

#we compute the normalized value with mean and variance
df5['norm'] = (df5['log_edits'] - df5['log_edits_mean'])/df5['log_edits_std']
df6 = df5[['views', 'norm']]
df6['groups'] = pd.cut(df6.views, np.arange(0, 10000, 100))
df6 = df6[['groups', 'norm']]
df6 = df6.groupby(['groups']).agg('mean').reset_index()
ax = df6.plot.line(x = 'groups', y = 'norm', logy = False)

#high quality articles
gf['year'] = pd.DatetimeIndex(gf['date']).year
gf['month'] = pd.DatetimeIndex(gf['date']).month
gf['log_high_quality_edits'] = np.log(gf['high_quality_edits'])
gf['age'] = 2021 - gf['year']

gf4 = gf[['year', 'month', 'log_high_quality_edits']]
gf4 = gf4.groupby(['year', 'month']).agg(['mean', std_dev]).reset_index()
gf5 = pd.merge(gf, gf4, how='left', left_on = ['year', 'month'], right_on = ['year', 'month'])
gf5.columns = ['num', 'name', 'high_quality_edits', 'editors', 'date', 'views', 'years', 'month', 'log_high_quality_edits', 'age', 'log_high_quality_edits_mean', 'log_high_quality_edits_std']

gf5['high_quality_norm'] = ((gf5['log_high_quality_edits'] - gf5['log_high_quality_edits_mean'])/gf5['log_high_quality_edits_std'])
gf6 = gf5[['views', 'high_quality_norm']]

gf6['groups'] = pd.cut(gf6.views, np.arange(0, 10000, 100))
gf6 = gf6[['groups', 'high_quality_norm']]
gf6 = gf6.groupby(['groups']).agg('mean').reset_index()
gf6.plot.line(ax = ax, x = 'groups', y = 'high_quality_norm', logy = False)

plt.show()