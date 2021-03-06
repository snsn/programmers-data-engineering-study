# -*- coding: utf-8 -*-
"""assignment-3.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1Y13vlKn8L9pygJDVNRPOCnGmy0rHBv_l
"""

# Commented out IPython magic to ensure Python compatibility.
import sqlalchemy
user = 'peter'
password = 'PeterWoW1!'
sql_conn_str = 'postgresql://{user}:{password}@grepp-data.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com:5439/dev'.format(
    user=user,
    password=password
)
engine = sqlalchemy.create_engine(sql_conn_str)
# %load_ext sql
# %sql postgresql://peter:PeterWoW1!@grepp-data.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com:5439/dev
echo=True

# Commented out IPython magic to ensure Python compatibility.
# %%sql
# 
# SELECT 
#   LEFT(stp.ts, 7) AS year_month, 
#   c.channelname, 
#   COUNT(DISTINCT usc.userid) AS uniqueUsers,
#   COUNT(
#       DISTINCT 
#         CASE WHEN stn.amount > 0
#           THEN usc.userid
#           ELSE NULL
#           END
#         ) AS paidUsers,
#   ROUND(paidUsers::float * 100 / NULLIF(uniqueUsers, 0), 2) AS conversionRate,
#   SUM(stn.amount) AS grossRevenue,
#   SUM(
#       CASE WHEN stn.refunded IS FALSE THEN stn.amount
#         ELSE 0
#         END
#   ) AS netRevenue
# FROM raw_data.channel c
# LEFT JOIN raw_data.user_session_channel usc
# ON c.channelname = usc.channel
# LEFT JOIN raw_data.session_timestamp stp
# ON stp.sessionid = usc.sessionid
# LEFT JOIN raw_data.session_transaction stn
# ON stn.sessionid = usc.sessionid
# GROUP BY 1, 2;

