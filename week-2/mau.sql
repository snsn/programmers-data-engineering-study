-- MAU QUERY

SELECT
    TO_CHAR(st.ts, 'yyyy-mm') AS monthly, 
    COUNT(DISTINCT usc.userid) AS active_users_count
FROM 
    raw_data.session_timestamp st
JOIN 
    raw_data.user_session_channel usc
ON 
    usc.sessionid = st.sessionid
GROUP BY 
    monthly
ORDER BY 
    monthly;

