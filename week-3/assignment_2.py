# -*- coding: utf-8 -*-
"""assignment-2.ipynb

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
sqlalchemy.create_engine(sql_conn_str)
# %load_ext sql
# %sql postgresql://peter:PeterWoW1!@grepp-data.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com:5439/dev

# Commented out IPython magic to ensure Python compatibility.
# %%sql
# 
# SELECT usc.userid, SUM(
#     CASE st.refunded
#       WHEN FALSE 
#       THEN st.amount
#       ELSE 0
#     END
# )
# FROM raw_data.user_session_channel usc
# JOIN raw_data.session_transaction st
# ON usc.sessionid = st.sessionid
# GROUP BY 1
# ORDER BY 2 DESC
# LIMIT 10;

