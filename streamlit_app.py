from logging import error
import streamlit as st
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import numpy as np

def getData():
    test = MongoClient('mongodb+srv://candidate:candidate2022@cluster0.p8u2o.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')
    col = test['master-catalogue']['test-temp']
    return pd.DataFrame(list(col.find({})))

data  = getData()

def parseTimestamp(timestamp):
    if(type(timestamp)==list): return parseTimestamp(timestamp[0])
    if(type(timestamp)==datetime): return timestamp
    if(type(timestamp)==float and np.isnan(timestamp)): return timestamp
    if timestamp.find('T')!=-1 :
        timestamp = timestamp.replace('T', ' ')
        timestamp = timestamp.replace('Z', '000')
    try :
        return datetime.fromisoformat(timestamp)
    except :
        raise error('Weird timestamp : ' + timestamp)

def weekFromDate(date):
    return date.toordinal()//7

def parseDataTimestamps():
    data['timestamp'] = data['timestamp'].map(parseTimestamp)
    data['approvalTimestamp'] = data['approvalTimestamp'].map(parseTimestamp)
    data['createdAt'] = data['createdAt'].map(parseTimestamp)

parseDataTimestamps()


createdAt = data[~data['createdAt'].isna()]['createdAt']
dates = pd.Series(map(lambda x : x.date(), createdAt))
weeks = pd.Series(map(lambda x : weekFromDate(x.date()), createdAt))

"""
# Question 1
## Daily doc creation : 
"""
table = data['createdAt'].groupby(dates).count()
table.name = 'Number of documents created'
st.line_chart(table)

"""## Weekly doc creation : 
(The x axis numbers haven't been mapped to a user friendly week format, apologies for that)
"""
table = data['createdAt'].groupby(weeks).count()
table.name = 'Number of documents created'
st.line_chart(table) 

"""
> There is a significant fluctuation in the creation of documents in a single week, which is hidden away in the week graph. With the given information, we can't rule out the possibility of this extra information being important. The fluctuations are quite significant in magnitude, and the overall weekly trend is still visible in the daily data. Therefore I would go with the daily version. 
"""

"""# 2. What fraction of documents have been approved"""
table = data[['approved']].groupby(dates).mean()
table.name = 'Fraction of documents approved'
st.line_chart(table)


"""# 3. TAT data"""
tatData = data[~data['createdAt'].isna() & ~data['approvalTimestamp'].isna()]
x = (tatData['approvalTimestamp'] - tatData['createdAt']).groupby(dates).mean().map(lambda delta : delta.total_seconds())
x.name = 'TAT (seconds)'
x
"""# 4. TAT data's distribution"""
st.line_chart(x)

"""# 5. For your chosen level of temporal aggregation, what fraction of documents have made use of the _miscellaneous_ field ?"""
"""
Unattempted. In the time I spent on this problem, I couldn't find the miscellaneous field. I haven't systematically searched all the subfields yet. 
"""

"""# 6. What fraction of documents were modified post approval for your time period"""

y = (data['timestamp'] - data['approvalTimestamp']).map(lambda delta : delta.total_seconds())
st.line_chart((y > 0).groupby(dates).mean())

"""## 6.a When was the relevant schema modification made ? """

"""
I reason as follows : 
1. It is possible to track post-approval modification by checking whether timestamp - approvalTimestamp is positive
2. This wasn't possible before the schema modification. Therefore, either timestamp, or approvalTimestamp was added in the modification
3. We search for the smallest timestamps of timestamp and approvalTimestamp fields. Here, we find that approvalTimestamp has a least value of numpy.datetime64('2021-11-23T11:00:40.000000000') and timestamp has a least value of numpy.datetime64('2021-11-20T08:49:15.668000000').
4. I'll now assume that these oldest values correspond approximately to the time when the fields were created. Therefore, that approvalTimestamp is likely the field that was added later, around 23-11-2021. This is also when the schema modification was probably made. 
"""